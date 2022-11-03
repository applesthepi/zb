#![feature(int_roundings)]

use std::{sync::{Arc, atomic::{AtomicBool, Ordering, AtomicU8}}, str::FromStr, io::{Write, SeekFrom}, hint};

use clap::Parser;
use debug_print::debug_println;
use lz4_flex::frame::FrameEncoder;
use tokio::{task::JoinHandle, sync::{RwLock, RwLockWriteGuard}, fs, io::{AsyncReadExt, self, AsyncSeekExt}};
use walkdir::WalkDir;

const IO_THREADS_DEFAULT: u16 = 1;
const OP_THREADS_DEFAULT: u16 = 3;
const OUTPUT_DEFAULT: &str = "<DIRECTORY>";
const BLOCK_SIZE: usize = 1_000_000;

#[derive(Parser)]
#[command(name = "ZB")]
#[command(author = "Jackson O. <applesthepi@gmail.com>")]
#[command(version = "1.0")]
#[command(about = "
ZB is a compression utility.

Guide:
 small: zb directory
medium: zb --op-max directory
 large: zb --heavy --op-max directory
  ssd+: zb --heavy --io-max directory", long_about = None)]
struct Args {
	/// Directory to compress. ZB compresses subdirectories as well.
	directory: String,

	/// Allows ZB to use much more overhead beforehand and during to accelerate large quantities of data. This will be suboptimal for small amounts of data.
	#[arg(long)]
	heavy: bool,

	/// [ OVERIDES IO_THREADS & OP_THREADS ] Maxes out threads, biased toward op_threads.
	#[arg(long, default_value_t = false)]
	io_max: bool,

	/// [ OVERIDES IO_THREADS & OP_THREADS ] Maxes out threads, biased toward io_threads.
	#[arg(long, default_value_t = false)]
	op_max: bool,

	/// [ REQUIRES HEAVY ] Reading threads during compression or write threads during decompression. Only very fast storage mediums take advantage of these threads.
	#[arg(long, default_value_t = IO_THREADS_DEFAULT)]
	io_threads: u16,

	/// Compression or decompression threads. Defaults to logical threads minus io threads.
	#[arg(long, default_value_t = OP_THREADS_DEFAULT)]
	op_threads: u16,

	/// Size of blocks being compressed indivisualy. Higher sizes increase compression ratios, consume more memory, and can be faster for large amounts of data.
	#[arg(short, long, default_value_t = BLOCK_SIZE)]
	block_size: usize,

	/// Custom output filename.
	#[arg(short, long, default_value_t = OUTPUT_DEFAULT.to_string())]
	output: String,
}

struct HeavyIO {
	pub ranges: Arc<RwLock<Vec<(usize, usize)>>>,
	pub sync: IOThreadSync,
}

struct IOThreadData {
	pub heavy_io: Option<HeavyIO>,
}

struct IOThreadSync {
	pub request: Arc<AtomicU8>,
	pub acquired: Arc<RwLock<Vec<u8>>>,
}

struct OPThreadData {

}

struct WriteCompression {
	data: Vec<u8>,
}

// impl Write for WriteCompression {
// 	fn write(&mut self, buf: &[u8]) -> io::Result<usize> {

// 	}

// 	fn flush(&mut self) -> io::Result<()> {
// 	}
// }

#[tokio::main]
async fn main() {
	let mut args = Args::parse();
	let mut io_thread_count = args.io_threads;
	let mut op_thread_count = args.op_threads;

	check_args(&mut args, &mut io_thread_count, &mut op_thread_count);
	debug_println!("compressing directory \"{}\"", args.directory);

	let mut io_threads: Vec<(JoinHandle<()>, Arc<RwLock<IOThreadData>>)> = Vec::with_capacity(io_thread_count as usize);
	let mut op_threads: Vec<(JoinHandle<()>, Arc<RwLock<OPThreadData>>)> = Vec::with_capacity(op_thread_count as usize);
	let mut fin_thread: JoinHandle<()>;

	// SPAWN THREADS

	debug_println!("spawning tasks...");
	for _ in 0..io_thread_count {
		let mut io_thread_data = IOThreadData {
			heavy_io: None
		};
		if args.heavy {
			io_thread_data.heavy_io = Some(HeavyIO {
				ranges: Arc::new(RwLock::new(Vec::new())),
				sync: IOThreadSync {
					request: Arc::new(AtomicU8::new(0)),
					acquired: Arc::new(RwLock::new(Vec::new()))
				}
			});
		}
		let data = Arc::new(RwLock::new(io_thread_data));
		let c_data = data.clone();
		let task = tokio::spawn(async move {
			task_io_thread(&c_data).await;
			// let file = entry.unwrap();
			// let file_path = file.path();
			// if file_path.is_dir() {
			// 	continue;
			// }
			// debug_println!("{}", file_path.display());
		});
		io_threads.push((task, data));
	}
	for _ in 0..op_thread_count {
		let data = Arc::new(RwLock::new(OPThreadData {

		}));
		let c_data = data.clone();
		let task = tokio::spawn(async move {
			let mut wa_data = c_data.write().await;
			task_op_thread(&mut wa_data);
		});
		op_threads.push((task, data));
	}
	fin_thread = tokio::spawn(async move {
		task_fin_thread();
	});
	debug_println!("...spawned tasks");

	// DISTRIBUTE READS

	debug_println!("distributing reads...");
	if args.heavy {
		heavy_distribute_read(&args, &mut io_threads).await;
	} else {
		light_distribute_read(&args, &mut io_threads).await;
	}
	debug_println!("...distributed reads");
}

async fn check_args(args: &mut Args, io_thread_count: &mut u16, op_thread_count: &mut u16) {
	let thread_count = num_cpus::get().max(4);

	if args.io_max && args.op_max {
		println!("--io-max & --op-max both can not be set. Using --op-max insead.");
		args.io_max = false;
	}
	if args.io_threads != IO_THREADS_DEFAULT && !args.heavy {
		println!("--heavy must be enabled when --io-threads is changed. Continueing WITH --heavy");
		args.heavy = true;
	}
	
	if args.op_max {
		*io_thread_count = 2;
		*op_thread_count = thread_count as u16 - *io_thread_count;
	} else if args.io_max {
		*io_thread_count = (thread_count as u16).div_floor(2) + 1;
		*op_thread_count = thread_count as u16 - *io_thread_count;
	}

	if *io_thread_count == 0 {
		*io_thread_count = 1;
	}
	if *op_thread_count == 0 {
		*op_thread_count = 1;
	}
}

async fn light_distribute_read(args: &Args, io_threads: &mut Vec<(JoinHandle<()>, Arc<RwLock<IOThreadData>>)>) {
	// let mod_inc: usize = 0;
	// let c_io_thread_data = io_threads[0].1.clone();
	// let mut wa_io_thread_data = c_io_thread_data.write().await;
	// let mut walker = WalkDir::new(args.directory.clone());
	// wa_io_thread_data. = (1, walker.into_iter().count());
}

async fn heavy_distribute_read(args: &Args, io_threads: &mut Vec<(JoinHandle<()>, Arc<RwLock<IOThreadData>>)>) {
	let mut walker = WalkDir::new(args.directory.clone());
	let mut read_buffer: Vec<u8> = Vec::with_capacity(args.block_size);
	// let mut overwrite_buffer: Vec<u8> = Vec::with_capacity(args.block_size);
	for file in walker.into_iter().skip(1) {
		let entry = file.unwrap();
		let path = entry.path();
		if entry.path().is_dir() {
			continue;
		}
		let path_str = path.to_str().unwrap();
		let mut fs_file = fs::File::open(path).await.unwrap();
		let file_size = fs_file.metadata().await.unwrap().len() as usize;
		let mut file_seek: usize = 0;

		if file_size + read_buffer.len() < args.block_size {
			fs_file.read_to_end(&mut read_buffer).await.unwrap();
			debug_println!("reading WHOLE file to buffer; result size: {} reading size: {}", read_buffer.len(), file_size);
			continue;
		}
		
		'file_split: loop {
			let overwrite: usize = read_buffer.len();
			if overwrite + (file_size - file_seek) < args.block_size {
				fs_file.seek(SeekFrom::Start(file_seek as u64)).await.unwrap();
				fs_file.read_to_end(&mut read_buffer).await.unwrap();
				debug_println!("reading REST of file to buffer; result size: {} reading size: {}", read_buffer.len(), file_size - file_seek);
				break 'file_split;
			}
			read_buffer.resize(args.block_size, 0);
			fs_file.seek(SeekFrom::Start(file_seek as u64)).await.unwrap();
			file_seek += fs_file.read_exact(&mut read_buffer[overwrite..args.block_size]).await.unwrap();
			debug_println!("reading BLOCK of file to buffer; result size: {} reading size: {}", read_buffer.len(), args.block_size - overwrite);

			// debug_println!("file: {} data_size: {} buffer len: {}", path_str, data_size, read_buffer.len());
			
			// FIND THREAD READY FOR BLOCK

			send_buffer(&mut read_buffer, io_threads).await;
		}
	}

	if !read_buffer.is_empty() {
		send_buffer(&mut read_buffer, io_threads).await;
	}
}

async fn send_buffer(read_buffer: &mut Vec<u8>, io_threads: &mut Vec<(JoinHandle<()>, Arc<RwLock<IOThreadData>>)>) {
	'find_tr: loop {
		for io_thread in io_threads.iter_mut() {
			let c_io_thread_data = io_thread.1.clone();
			let ra_io_thread_data = c_io_thread_data.read().await;
			let heavy = ra_io_thread_data.heavy_io.as_ref().unwrap();
			if let Err(_) = heavy.sync.request.compare_exchange(
				1,
				2,
				Ordering::SeqCst,
				Ordering::SeqCst
			) {
				continue;
			}

			let c_acquired = heavy.sync.acquired.clone();
			let mut wa_acquired = c_acquired.write().await;
			wa_acquired.extend(read_buffer.iter());
			drop(wa_acquired);
			// drop(c_acquired);
			heavy.sync.request.store(
				3,
				Ordering::SeqCst
			);
			read_buffer.clear();
			debug_println!("dumped WHOLE block to thread");
			break 'find_tr;
		}
	}
}

async fn task_io_thread(c_thread_data: &Arc<RwLock<IOThreadData>>) {
	let ra_thread_data = c_thread_data.read().await;
	if ra_thread_data.heavy_io.is_none() {


		return;
	}

	let heavy = ra_thread_data.heavy_io.as_ref().unwrap();
	heavy.sync.request.store(
		1,
		Ordering::SeqCst
	);
	
	loop {
		if let Err(_) = heavy.sync.request.compare_exchange(
			3,
			0,
			Ordering::SeqCst,
			Ordering::SeqCst
		) {
			hint::spin_loop();
			continue;
		}

		let c_acquired = heavy.sync.acquired.clone();
		let mut wa_acquired = c_acquired.write().await;

		debug_println!("thread captured block size:\n{}", wa_acquired.len());
		wa_acquired.clear();

		drop(wa_acquired);

		heavy.sync.request.store(
			1,
			Ordering::SeqCst
		);
	}
}

fn task_op_thread(thread_data: &mut RwLockWriteGuard<OPThreadData>) {

}

fn task_fin_thread() {

}