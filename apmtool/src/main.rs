use std::fs;
use std::path::PathBuf;
use anyhow::{Context, Result, anyhow};
use clap::{Subcommand, Parser};
use apm::{ApmMap};

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    op: Cmd,
}

#[derive(Subcommand, Clone)]
enum Cmd {
    /// Prints out most important information about the drive
    Print {
        /// Path to the device
        file: PathBuf,
        /// Whether to print out all the information
        #[arg(short, long)]
        verbose: bool,
    },
    /// Replaces partition data with contents of a file
    ReplacePartition {
        /// Path to the whole device
        file: PathBuf,
        /// Path to partition data
        data: PathBuf,
        /// Number of partition as identified using 'print' subcommand
        num: u8
    },
    /// Saves a partition data to a file
    DumpPartition {
        file: PathBuf,
        /// Number of partition as identified using 'print' subcommand
        num: u8,
        /// Path to save the partition to
        path: PathBuf
    },
    /// Saves driver data to a file
    DumpDriver {
        file: PathBuf,
        /// Number of driver as identified using 'print' subcommand
        num: u8,
        /// Path to save the driver to
        path: PathBuf
    },
    /// Creates a new file with specified partition and driver data
    Create {
        file: PathBuf,
        /// The size of the file, will be rounded up to 512 byte increments
        #[arg(short, value_parser = size_binary)]
        size: u32,
        #[arg(short)]
        /// Path to partition data, will be inserted in order
        partition: Vec<PathBuf>,
        #[arg(short)]
        /// Path to driver data, will be inserted in order
        driver: Vec<PathBuf>,
        #[arg(long)]
        /// cursed
        driver43: Option<PathBuf>,
    },
}

fn size_binary(v: &str) -> Result<u32, anyhow::Error> {
    Ok(parse_size::Config::new()
        .with_binary()
        .parse_size(v)
        .map(|val| u32::try_from(val))??)
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.op {
        Cmd::Print{file, verbose} => {
            let input = fs::read(&file)
                .context("Failed to read the input file")?;
            let drive = ApmMap::decode(input)
                .context("Failed parsing the input file as APM data")?;
            println!("Block size: {} bytes", drive.block_size());
            println!("Drive size: {} bytes", drive.blk_count() * 512);
            if verbose {
                println!("Device type: {}", drive.dev_type());
                println!("Device ID: {}", drive.dev_id());
                println!("Reserved data: {}", drive.data());
            }
            for (i, (d, _)) in drive.drivers().enumerate() {
                println!("Driver {}:", i);
                println!("\tStart: {} blocks", d.start());
                println!("\tSize: {} blocks", d.size());
                println!("\tType: {}", d.ty());
            }
            for (i, (p, _)) in drive.partitions().enumerate() {
                println!("Partition {}:", i);
                println!("\tName: '{}'", p.name());
                println!("\tType: '{}'", p.part_type());
                println!("\tStart: {} blocks", p.start());
                println!("\tLength: {} blocks", p.length());
                if verbose {
                    println!("\tData start: {} blocks", p.data_start());
                    println!("\tData length: {} blocks", p.data_size());
                    println!("\tStatus: 0x{:08x}", p.status());
                    println!("\tBoot code start: {} blocks", p.boot_start());
                    println!("\tBoot code size: {} blocks", p.boot_size());
                    println!("\tBoot load address: 0x{:08x}", p.boot_load_address());
                    println!("\tBoot entry point: 0x{:08x}",  p.boot_entry());
                    println!("\tBoot code checksum: 0x{:08x}", p.boot_checksum());
                    println!("\tProcessor type: '{}'", p.proc_type());
                }
            }
        },
        Cmd::DumpPartition{file, num, path} => {
            let input = fs::read(&file)
                .context("Failed to read the input file")?;
            let drive = ApmMap::decode(input)
                .context("Failed parsing the input file as APM data")?;
            let data = drive.partition_data(num as usize)
                .ok_or(anyhow!("Failed to find partition"))?;
            fs::write(&path, data)
                .context("Failed to write data of partition")?;
            },
        Cmd::ReplacePartition{file, num, data} => {
            let data = fs::read(&data)
                .context("Failed to read the input data file")?;
            let input = fs::read(&file)
                .context("Failed to read the input file")?;
            let mut drive = ApmMap::decode(input)
                .context("Failed parsing the input file as APM data")?;
            drive.partition_data_mut(num as usize)
                .ok_or(anyhow!("Failed to find partition"))?
                .copy_from_slice(&data);
            fs::write(&file, drive.encode()?)
                .context("Failed to update the input file")?;
        }
        Cmd::DumpDriver{file, num, path} => {
            let input = fs::read(file)
                .context("Failed to read the input file")?;
            let drive = ApmMap::decode(input)
                .context("Failed parsing the input file as APM data")?;
            let data = drive.drivers()
                .enumerate()
                .find(|(p_num, _)| *p_num == num as usize)
                .inspect(|(_, (info, _))| println!("Dumping {} blocks from {}", info.size(), info.start()))
                .map(|(_, (_, d))| d)
                .ok_or(anyhow!("Unknown driver number {}", num))?;
            fs::write(&path, data)
                .context("Failed to write data of partition")?;
        },
        Cmd::Create{file, size, partition, driver, driver43} => {
            let size = (size + 0x1ff & !0x1ff)/512;
            let mut drive = ApmMap::new(size);
            if let Some(p) = &driver43 {
                let data = fs::read(p).unwrap();
                drive.push_driver(1, &data[..(19*512)])?;
                drive.push_partition_at("nochecksumplz", "Apple_Driver43", "68000", &data, 64)?;
            }
            for d in driver {
                let data = fs::read(&d)
                    .context("Failed to read driver data")?;
                drive.push_driver(1, &data)
                    .context("Failed to add the driver to drive")?;
            }
            for d in partition {
                let data = fs::read(&d)
                    .context("Failed to read partition data")?;
                drive.push_partition("MacOS", "Apple_HFS", &data)
                    .context("Failed to add the partition to drive")?;
            }
            fs::write(&file, drive.encode().context("Failed encoding the drive")?)
                .context("Failed saving the output file")?;
            println!("{:#?}", drive);
        },
    }

    Ok(())
}
