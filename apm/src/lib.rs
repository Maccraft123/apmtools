use std::io;
use derivative::Derivative;
use deku::prelude::*;
use thiserror::Error;

#[derive(Clone, Debug, DekuRead, DekuWrite)]
#[deku(endian = "big", magic = b"ER")]
pub struct DriverDescriptorBlock {
    /// The block size of the device, in bytes
    block_size: u16,
    /// Size of the device, in blocks
    blk_count: u32,
    /// Reserved
    dev_type: u16,
    /// Reserved
    dev_id: u16,
    /// Reserved
    data: u32,
    /// Number of drivers installed on thte disk
    #[deku(update = "self.drivers.len()")]
    driver_count: u16,
    #[deku(count = "driver_count")]
    drivers: Vec<DriverData>,
}

impl DriverDescriptorBlock {
    pub fn push_driver_data(&mut self, data: DriverData) {
        self.drivers.push(data);
        self.driver_count += 1;
    }
    pub fn blk_count(&self) -> u32 {
        self.blk_count
    }
    pub fn set_blk_count(&mut self, blk_count: u32) {
        self.blk_count = blk_count;
    }
    pub fn with_blk_count(mut self, blk_count: u32) -> Self {
        self.blk_count = blk_count;
        self
    }
}

impl Default for DriverDescriptorBlock {
    fn default() -> Self {
        Self {
            block_size: 0x200,
            blk_count: 0,
            dev_type: 1,
            dev_id: 1,
            data: 0,
            driver_count: 0,
            drivers: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, DekuRead, DekuWrite)]
#[deku(endian = "big", ctx = "_: deku::ctx::Endian")]
pub struct DriverData {
    /// Physical block of this device driver
    start: u32,
    /// Size in 512-byte blocks
    size: u16,
    /// Operating system or processor supported by the driver
    system_type: u16,
}

impl DriverData {
    pub fn new(start: u32, size: u16, system_type: u16) -> Self {
        Self { start, size, system_type }
    }
    pub fn start(&self) -> u32 {
        self.start
    }
    pub fn size(&self) -> u16 {
        self.size
    }
    pub fn ty(&self) -> u16 {
        self.system_type
    }
}

#[derive(Clone, Debug, DekuRead, DekuWrite)]
#[deku(endian = "big")]
pub struct PartitionEntry {
    #[deku(assert = "*sig == 0x504d || *sig == 5453")]
    /// Magic bytes, 0x504d or 0x5453
    sig: u16,
    #[deku(pad_bytes_before = "2")]
    partition_count: u32,
    /// Starting block of this partition
    start: u32,
    /// Length in blocks
    length: u32,
    #[deku(
        reader = "PartitionEntry::read_string::<32, R>(deku::reader)",
        writer = "PartitionEntry::write_string::<32, W>(deku::writer, &self.name)"
    )]
    name: String,
    #[deku(
        reader = "PartitionEntry::read_string::<32, R>(deku::reader)",
        writer = "PartitionEntry::write_string::<32, W>(deku::writer, &self.ty)"
    )]
    /// Partition type
    ty: String,
    /// Start of data in blocks
    data_start: u32,
    /// Length of data in blocks
    data_count: u32,
    status: u32,
    /// Start of boot code in blocks
    boot_start: u32,
    /// Size of boot code in blocks
    boot_size: u32,
    /// Load address of boot code
    boot_load_address: u32,
    #[deku(pad_bytes_before = "4")]
    /// Boot code entry point
    boot_entry: u32,
    #[deku(pad_bytes_before = "4")]
    /// Checksum of the boot code, only used when name starts with "Maci"
    boot_checksum: u32,
    #[deku(
        reader = "PartitionEntry::read_string::<16, R>(deku::reader)",
        writer = "PartitionEntry::write_string::<16, W>(deku::writer, &self.proc_type)"
    )]
    proc_type: String,
}

impl PartitionEntry {
    fn read_string<const LEN: usize, R: io::Read>(reader: &mut deku::reader::Reader<R>) -> Result<String, DekuError> {
        let mut buf = [0u8; LEN];
        reader.read_bytes(LEN, &mut buf)?;
        Ok(buf.into_iter().take_while(|v| *v != 0).map(|c| c as char).collect::<String>())
    }
    fn write_string<const LEN: usize, R: io::Write>(writer: &mut deku::writer::Writer<R>, val: &str) -> Result<(), DekuError> {
        let mut ret = [0u8; LEN];
        for (i, ch) in val.chars().enumerate() {
            ret[i] = ch as u8;
        }
        writer.write_bytes(&ret)
    }
    pub fn new() -> Self {
        Self {
            sig: 0x504d,
            partition_count: 0x0,
            start: 0x0,
            length: 0x0,
            name: String::new(),
            ty: String::new(),
            data_start: 0x0,
            data_count: 0x0,
            status: 0xb7,
            boot_start: 0x0,
            boot_size: 0x0,
            boot_load_address: 0x0,
            boot_entry: 0x0,
            boot_checksum: 0x0,
            proc_type: String::new(),
        }
    }
    pub fn data_start(&self) -> u32 { self.data_start }
    pub fn data_size(&self) -> u32 { self.data_count }
    pub fn boot_start(&self) -> u32 { self.boot_start }
    pub fn boot_size(&self) -> u32 { self.boot_size }
    pub fn boot_load_address(&self) -> u32 { self.boot_load_address }
    pub fn boot_entry(&self) -> u32 { self.boot_entry }
    pub fn with_checksum(mut self, checksum: u32) -> Self { self.boot_checksum = checksum; self }
    pub fn with_boot_code_size(mut self, size: u32) -> Self { self.boot_size = size; self }
    pub fn boot_checksum(&self) -> u32 { self.boot_checksum }
    pub fn part_type(&self) -> &str { &self.ty }

    pub fn proc_type(&self) -> &str {
        &self.proc_type
    }
    pub fn with_proc_type(mut self, t: impl Into<String>) -> Self {
        self.proc_type = t.into();
        self
    }
    pub fn set_proc_type(&mut self, t: impl Into<String>) {
        self.proc_type = t.into();
    }
    pub fn with_type(mut self, name: impl Into<String>) -> Self {
        self.ty = name.into();
        self
    }
    pub fn set_type(&mut self, name: impl Into<String>) {
        self.ty = name.into();
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }
    pub fn set_name(&mut self, name: impl Into<String>) {
        self.name = name.into();
    }
    pub fn status(&self) -> u32 {
        self.status
    }
    pub fn with_status(mut self, status: u32) -> Self {
        self.status = status;
        self
    }
    pub fn set_status(&mut self, status: u32) {
        self.status = status;
    }
    pub fn partition_count(&self) -> u32 {
        self.partition_count
    }
    pub fn with_partition_count(mut self, cnt: u32) -> Self {
        self.partition_count = cnt;
        self
    }
    pub fn set_partition_count(&mut self, cnt: u32) {
        self.partition_count = cnt;
    }
    pub fn length(&self) -> u32 {
        assert_eq!(self.data_count, self.length);
        self.length
    }
    pub fn with_length(mut self, len: u32) -> Self {
        self.length = len;
        self.data_count = len;
        self
    }
    pub fn set_length(&mut self, len: u32) {
        self.length = len;
        self.data_count = len;
    }
    pub fn start(&self) -> u32 {
        self.start
    }
    pub fn with_start(mut self, start: u32) -> Self {
        self.start = start;
        self
    }
    pub fn set_start(&mut self, start: u32) {
        self.start = start;
    }
}

#[derive(Error, Debug, Clone)]
pub enum ApmError {
    #[error("Parse/Encode error")]
    Deku(#[from] deku::DekuError),
    #[error("Failed to locate a sufficiently sized empty space")]
    NoSpace,
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct ApmMap {
    update_driver_desc: bool,
    driver_desc: DriverDescriptorBlock,
    update_partition_table: bool,
    partitions: Vec<PartitionEntry>,
    #[derivative(Debug = "ignore")]
    raw_data: Vec<u8>,
}

fn apple_checksum(data: &[u8]) -> u16 {
    let mut ret: u16 = 0;
    for b in data.iter() {
        ret = ret.wrapping_add(*b as u16);
        ret = ret.rotate_left(1);
    }
    if ret == 0 { ret = 0xffff }
    assert_eq!(ret, 0x885f);

    ret
}

impl ApmMap {
    pub fn new(blocks: u32) -> Self {
        Self {
            update_driver_desc: true,
            driver_desc: DriverDescriptorBlock::default().with_blk_count(blocks),
            update_partition_table: true,
            partitions: vec![
                PartitionEntry::new()
                    .with_start(1)
                    .with_length(0x3f)
                    .with_partition_count(1)
                    .with_name("Apple")
                    .with_type("Apple_partition_map"),
            ],
            raw_data: vec![0; (blocks as usize)*512],
        }
    }
    pub fn block_size(&self) -> u16 { self.driver_desc.block_size }
    pub fn blk_count(&self) -> u32 { self.driver_desc.blk_count }
    pub fn dev_type(&self) -> u16 { self.driver_desc.dev_type }
    pub fn dev_id(&self) -> u16 { self.driver_desc.dev_id }
    pub fn data(&self) -> u32 { self.driver_desc.data }
    pub fn driver_bytes(&self, num: usize) -> &[u8] {
        let start = (self.driver_desc.drivers[num].start * 512) as usize;
        let size = (self.driver_desc.drivers[num].size * 512) as usize;
        &self.raw_data[start..][..size]
    }
    pub fn push_partition<N, T>(&mut self, name: N, ty: T, data: &[u8]) -> Result<(), ApmError>
    where
        N: Into<String>, T: Into<String>,
    {
        let size = (data.len() + 0x1ff & !0x1ff)/512;
        let start = self.find_hole(size as u32)?;
        let entry = PartitionEntry::new()
            .with_start(start)
            .with_length(size as u32)
            .with_name(name)
            .with_type(ty);
        self.partitions.push(entry);
        self.raw_data[(start*512) as usize..][..data.len()].copy_from_slice(data);
        self.update_partition_count();
        self.update_partition_table = true;
        Ok(())
    }
    pub fn push_partition_at<N, T, P>(&mut self, name: N, ty: T, proc: P, data: &[u8], start: u32) -> Result<(), ApmError>
    where
        N: Into<String>, T: Into<String>, P: Into<String>,
    {
        let size = (data.len() + 0x1ff & !0x1ff)/512;
        let entry = PartitionEntry::new()
            .with_start(start)
            .with_length(size as u32)
            .with_name(name)
            .with_type(ty)
            .with_checksum(0xf624)
            .with_boot_code_size(9392)
            .with_proc_type(proc);
        println!("checksum {:04x}", apple_checksum(data));
        self.partitions.push(entry);
        self.raw_data[(start*512) as usize..][..data.len()].copy_from_slice(data);
        self.update_partition_count();
        self.update_partition_table = true;
        Ok(())
    }
    pub fn update_partition_count(&mut self) {
        let count = self.partitions.len();
        for p in self.partitions.iter_mut() {
            p.partition_count = count as u32;
        }
    }
    pub fn push_driver(&mut self, ty: u16, data: &[u8]) -> Result<(), ApmError> {
        let size = (data.len() + 0x1ff & !0x1ff)/512;
        let start = self.find_hole(size as u32)?;
        self.driver_desc.push_driver_data(DriverData::new(start, size as u16, ty));
        self.raw_data[(start*512) as usize..][..data.len()].copy_from_slice(data);
        self.update_driver_desc = true;
        Ok(())
    }
    fn find_hole(&self, size: u32) -> Result<u32, ApmError> {
        let mut hole = 0x1;
        for (p, _) in self.partitions_used() {
            hole = p.start + p.length;
        }
        if hole + size > self.raw_data.len() as u32/512 {
            Err(ApmError::NoSpace)
        } else {
            Ok(hole)
        }
    }
    pub fn drivers(&self) -> impl Iterator<Item = (&DriverData, &[u8])> {
        self.driver_desc.drivers.iter()
            .map(|driver| {
                (driver, &self.raw_data[(driver.start * 512) as usize..][..(driver.size * 512) as usize])
            })
    }
    pub fn partition_data(&self, idx: usize) -> Option<&[u8]> {
        self.partitions.iter()
            .enumerate()
            .find(|(i, _)| *i == idx)
            .map(|(_, p)| (p.start, p.length))
            .map(|(start, length)| &self.raw_data[(start*512) as usize..][..(length*512) as usize])
    }
    pub fn partition_data_mut(&mut self, idx: usize) -> Option<&mut [u8]> {
        self.partitions.iter()
            .enumerate()
            .find(|(i, _)| *i == idx)
            .map(|(_, p)| (p.start, p.length))
            .map(|(start, length)| &mut self.raw_data[(start*512) as usize..][..(length*512) as usize])
    }
    pub fn partitions_used(&self) -> impl Iterator<Item = (&PartitionEntry, &[u8])> {
        self.partitions()
            .filter(|(p, _)| p.part_type() != "Apple_Free")
    }
    pub fn partitions(&self) -> impl Iterator<Item = (&PartitionEntry, &[u8])> {
        self.partitions.iter()
            .map(|p| {
                (p, &self.raw_data[(p.start*512) as usize..][..(p.length * 512) as usize] )
            })
    }
    pub fn raw(&self) -> &[u8] {
        &self.raw_data
    }
    pub fn decode(data: Vec<u8>) -> Result<Self, ApmError> {
        let mut iter = data.chunks(512).enumerate();
        let mut partitions = Vec::new();
        let driver_bytes = iter.next().unwrap().1;
        let driver_desc = DriverDescriptorBlock::from_bytes((driver_bytes, 0))?.1;
        for (i, block) in iter {
            let (_, entry) = PartitionEntry::from_bytes((block, 0))?;
            let entry_count = entry.partition_count as usize;
            partitions.push(entry);
            if i == entry_count {
                break;
            }
        }

        Ok(Self {
            update_driver_desc: false,
            driver_desc,
            update_partition_table: false,
            partitions,
            raw_data: data,
        })
    }
    pub fn encode(&mut self) -> Result<&[u8], ApmError> {
        if self.update_driver_desc {
            let block0 = self.driver_desc.to_bytes()?;
            self.raw_data[..512][..block0.len()].copy_from_slice(&block0);
        }

        if self.update_partition_table {
            self.update_partition_count();
            for (i, entry) in self.partitions.iter().enumerate() {
                let bytes = entry.to_bytes()?;
                self.raw_data[512+i*512..][..bytes.len()].copy_from_slice(&bytes);
            }
        }

        Ok(&self.raw_data)
    }
}
