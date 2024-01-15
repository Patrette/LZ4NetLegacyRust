use std::cmp::*;
use std::io::Cursor;
use bitflags::bitflags;
use bytes::{Buf, BufMut, BytesMut};
use bytes_varint::{VarIntError, VarIntSupport, VarIntSupportMut};
use lz4_flex::block::DecompressError;
use crate::ChunkResult::*;
use crate::DSError::CorruptedOverflow;

extern crate lz4_flex as lz4;

bitflags! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct ChunkFlags: u32 {
        const None = 0x00;
        const Compressed = 0x01;
        const Passes = 0x02;
    }
}

pub fn decode_stream(mut data: Cursor<&mut [u8]>, max_output: usize) -> Result<Vec<u8>, DSError>
{
    let out_size = calc_dc_size(&mut data)?;
    if out_size > max_output
    {
        return Err(DSError::Overflow(out_size, max_output))
    }
    let out = Cursor::new(vec![0u8; out_size]);
    let mut state = StreamDecodeState{
        output: out,
        input: data
    };

    loop {
        if get_chunk(&mut state)? {break}
    }
    Ok(state.output.into_inner())
}

pub(crate) struct StreamDecodeState<'a>
{
    pub input: Cursor<&'a mut [u8]>,
    pub output: Cursor<Vec<u8>>,
}

pub fn encode_stream(data: &mut impl Buf) -> Result<Vec<u8>, DSError>
{
    let mut result: BytesMut = BytesMut::new();

    let mut buffer: &[u8];

    loop {
        if data.remaining() == 0
        {
            break;
        }
        let len: usize = min(data.remaining(), BLOCKSIZE);
        buffer = &data.chunk()[0..len];
        write_chunk(&mut result, buffer);
        data.advance(len);
    }
    Ok(result.to_vec())
}

#[derive(Debug)]
pub enum DSError
{
    LZ4(DecompressError),
    CorruptedOverflow,
    Overflow(usize, usize),
    VarintFail
}
pub enum ChunkResult
{
    Overflow,
    VarintFail,
    LZ4Fail(DecompressError)
}

impl From<VarIntError> for ChunkResult
{
    fn from(value: VarIntError) -> Self {
        match value {
            VarIntError::NumericOverflow => VarintFail,
            VarIntError::BufferUnderflow => VarintFail
        }
    }
}

impl From<ChunkResult> for DSError
{
    fn from(value: ChunkResult) -> Self {
        match value {
            Overflow => CorruptedOverflow,
            VarintFail => DSError::VarintFail,
            LZ4Fail(ex) => DSError::LZ4(ex)
        }
    }
}

impl From<DecompressError> for ChunkResult
{
    fn from(value: DecompressError) -> Self {
        LZ4Fail(value)
    }
}

fn calc_dc_size(data: &mut Cursor<&mut [u8]>) -> Result<usize, ChunkResult>
{
    let mut count: usize = 0;
    let prev_pos = data.position();
    loop {
        if data.remaining() == 0 {
            data.set_position(prev_pos);
            return Ok(count)
        }
        let flags_raw = data.get_u64_varint()? as u32;
        let flags: ChunkFlags = ChunkFlags::from_bits_retain(flags_raw);
        let is_compressed = flags.contains(ChunkFlags::Compressed);
        let original_length = data.get_u64_varint()? as usize;
        let length: usize =
            if is_compressed
            {
                data.get_u64_varint()? as i32 as usize
            } else {
                original_length
            };
        if length > data.remaining()
        {
            return Err(Overflow);
        }
        count = match count.checked_add(original_length) {
            None => return Err(Overflow),
            Some(x) => x
        };
        data.advance(length);
    }
}

fn get_chunk(state: &mut StreamDecodeState) -> Result<bool, ChunkResult>
{
    if state.input.remaining() == 0 {return Ok(true)}
    let flags_raw = state.input.get_u64_varint()? as u32;
    let flags: ChunkFlags = ChunkFlags::from_bits_retain(flags_raw);
    let is_compressed = flags.contains(ChunkFlags::Compressed);
    let original_length = state.input.get_u64_varint()? as usize;
    let length: usize =
        if is_compressed
        {
            state.input.get_u64_varint()? as usize
        }
        else
        {
            original_length
        };
    if length > state.input.remaining() || original_length > state.output.remaining()
    {
        return Err(Overflow);
    }
    let cv = &state.input.chunk()[..length];
    let pos = state.output.position() as usize;
    let out = &mut state.output.get_mut()[pos..original_length + pos];
    if is_compressed {
        if lz4_flex::block::decompress_into(cv, out)? != original_length
        {
            return Err(Overflow)
        }
    }
    else
    {
        out.copy_from_slice(cv);
    };
    state.input.advance(length);
    state.output.advance(original_length);
    Ok(false)
}

pub const BLOCKSIZE: usize = 1024*1024;

pub fn write_chunk(data: &mut BytesMut, input: &[u8])
{
    data.put_u64_varint(0x01);
    data.put_u64_varint(input.len() as u64);
    let comp: Vec<u8> = lz4_flex::block::compress(input);
    data.put_u64_varint(comp.len() as u64);
    data.put(comp.as_slice());
}