use tracing::error;

use anyhow::Result;

#[derive(Clone, Eq, PartialEq)]
pub struct BitField {
    /// The actual value of the BitField. Stored as `Vec<u8>` since these BitFields can be anylength
    bits: Vec<u8>,
    /// The number of valid/settable bits
    pub settable: u64,
}
impl BitField {
    pub fn new(bytes: Vec<u8>, settable: u64) -> Result<Self, &'static str> {
        if settable as usize > 8 * bytes.len() {
            return Err("not enough bytes supplied in bitfield");
        } else if (settable as usize) < bytes.len() / 8 {
            return Err("not enough settable bytes");
        }
        let mut ones = 0;
        let mut furthest_left = 0;
        for (i, &byte) in bytes.iter().enumerate() {
            for bit in 0..8 {
                if (1 << bit) as u8 & byte != 0 {
                    furthest_left = (8 * i) + (7 - bit);
                    ones += 1;
                }
            }
        }

        if furthest_left >= settable as usize || ones > settable {
            error!("furthest_left={furthest_left}");
            error!("settable={settable}");
            return Err("too many ones set in bitfield");
        }

        Ok(Self {
            bits: bytes,
            settable,
        })
    }

    pub fn update(&mut self, other: &Self) -> Result<(), std::io::Error> {
        if self.settable != other.settable {
            return Err(std::io::Error::other("Number of settable bits differ"));
        }

        for (my_bit, other_bit) in self.bits.iter_mut().zip(other.bits.iter()) {
            *my_bit |= other_bit;
        }

        Ok(())
    }

    pub fn from_slice(bits: &[u8]) -> Self {
        Self {
            bits: bits.to_vec(),
            settable: bits.len() as u64 * 8,
        }
    }

    pub fn with_settable(settable: u64) -> Self {
        Self {
            settable,
            bits: vec![0u8; settable.div_ceil(8) as usize], /* computes the number of bytes necessary for the required number of bits */
        }
    }

    pub fn is_set(&self, index: usize) -> Result<bool, &'static str> {
        if index > self.settable as usize {
            return Err("Index out of bounds");
        }
        let byte_index = index / 8;
        let bit_index = index % 8;
        if byte_index >= self.bits.len() {
            return Err("Index out of bounds");
        }
        let mask = 1 << (7 - bit_index);
        Ok(self.bits[byte_index] & mask != 0)
    }

    pub fn set(&mut self, index: usize, value: bool) -> Result<bool, &'static str> {
        if index > self.settable as usize {
            return Err("Index out of bounds");
        }
        let byte_index = index / 8;
        let bit_index = index % 8;
        if byte_index >= self.bits.len() {
            return Err("Index out of bounds");
        }
        let mask = 1 << (7 - bit_index);
        let prev = (self.bits[byte_index] & mask) == mask;
        if value {
            self.bits[byte_index] |= mask;
        } else {
            self.bits[byte_index] &= !mask;
        }
        Ok(prev)
    }

    /// Determines if another set has any bits set that `self` does not have set
    /// This returns an error if the bitfields are of different sizes
    pub fn has_other(&self, other: &BitField) -> Result<bool, &'static str> {
        if self.settable != other.settable {
            return Err("BitFields do not have the same number of settable bits");
        }
        for (&self_byte, &other_byte) in self.bits.iter().zip(other.bits.iter()) {
            if (!self_byte) & other_byte != 0 {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Returns a vector containing all indices that are set in the piece
    pub fn set_bits(&self) -> Vec<usize> {
        let mut indices = vec![];
        for (i, &byte) in self.bits.iter().enumerate() {
            for bit in (0..8).rev() {
                if (1 << bit) & byte != 0 {
                    indices.push((8 * i) + (7 - bit));
                }
            }
        }
        indices
    }

    /// Determines if `self` is set for at least every bit that `other` is set for
    /// This fails if the bitfields are of different sizes
    pub fn contains(&self, other: &BitField) -> Result<bool, &'static str> {
        if self.settable != other.settable {
            return Err("BitFields do not have the same number of bytes");
        }
        for (&self_byte, &other_byte) in self.bits.iter().zip(other.bits.iter()) {
            if self_byte & other_byte != other_byte {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

impl std::fmt::Debug for BitField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BitField {{ bits: ")?;
        for (i, byte) in self.bits.iter().enumerate() {
            if i > 0 {
                write!(f, "_")?;
            }
            write!(f, "{:08b}", byte)?;
        }
        write!(f, " }}")
    }
}
