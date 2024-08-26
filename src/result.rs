/*
 *  This file is part of AndroidIDE.
 *
 *  AndroidIDE is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  AndroidIDE is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *   along with AndroidIDE.  If not, see <https://www.gnu.org/licenses/>.
 */

use std::fmt::Display;

use crate::LevelHash;

pub type LevelResult<T, E> = Result<T, E>;

pub type LevelInitResult = Result<LevelHash, LevelInitError>;

pub type LevelInsertionResult = LevelResult<(), LevelInsertionError>;

pub type LevelExpansionResult = LevelResult<(), LevelExpansionError>;

pub type LevelUpdateResult = LevelResult<Vec<u8>, LevelUpdateError>;

pub type LevelRemapResult = Result<(), LevelMapError>;

pub type LevelClearResult = LevelRemapResult;

/// An I/O error in level hash.
#[derive(Debug)]
pub struct StdIOError {
    message: Option<String>,
    error: std::io::Error,
}

impl StdIOError {
    /// Create a new IO error with the given message.
    pub fn with_message(message: String, error: std::io::Error) -> Self {
        Self::new(Some(message), error)
    }

    /// Create a new IO error with an optional message.
    pub fn new(message: Option<String>, error: std::io::Error) -> Self {
        StdIOError { message, error }
    }
}

impl Display for StdIOError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "I/O error: {} reason: {}",
            self.message
                .as_ref()
                .unwrap_or(&"unknown error".to_string()),
            self.error
        )
    }
}

/// Level hash error enumeration.
#[derive(Debug)]
pub enum LevelInitError {
    /// An error caused due to IO operations.
    IOError(StdIOError),

    /// An error occured during memory mapping.
    MmapError(LevelMapError),

    /// An error caused due to invalid arguments.
    InvalidArg(String),
}

/// Error occured during an insertion operation in level hash.
#[derive(Debug)]
pub enum LevelInsertionError {
    /// Occurs when an entry with a given key already exists.
    DuplicateKey,

    /// Occurs when the auto-expand fails.
    ExpansionFailure,

    /// Occurs when the level hash is full.
    LevelOverflow,

    /// Occurs when there is an error memory mapping/remapping a file.
    MmapError(LevelMapError),

    /// Occurs when the level hash fails to move an entry between buckets to
    /// make space for a new entry.
    MovementFailure,

    /// Occurs when an entry cannot be inserted into the level hash. There might be
    /// multiple reasons for this error to occur, but the most common one is when the
    /// level hash reaches a certain load factor (usually >0.9) and the level hash
    /// cannot be expanded further resulting in hash collisions for the given key.
    InsertionFailure,
}

#[derive(Debug)]
pub enum LevelUpdateError {
    /// Error indicating that the slot for the given key was not found.
    SlotNotFound,

    /// Error indicating the slot was found, but was empty (did not point to a value).
    SlotEmpty,

    /// Error indicating the entry being updated was not occupied.
    EntryNotOccupied,

    /// Error indicating there was an error inserting the new value to the entry.
    InsertionErr(LevelInsertionError),
}

#[derive(Debug)]
pub enum LevelExpansionError {
    /// Occurs when the level hash already has the maximum level size and cannot be expanded further
    MaxLevelSizeReached,

    /// Occurs when level hash fails to expand the memory mapped file.
    MmapError(LevelMapError),

    /// An update error occurred while moving slots
    UpdateError(LevelUpdateError),

    /// Occurs when trying to expand the level hash while another hash-level operation is in progress.
    /// This hash-level operation can be another expand operation or the clear operation.
    ConcurrentModificationError
}

/// Error occured during memory-mapping a file.
#[derive(Debug)]
pub enum LevelMapError {
    IOError(StdIOError),
}

pub trait IntoLevelIOErr<T> {
    fn into_lvl_io_err(self) -> Result<T, StdIOError>;
    fn into_lvl_io_e_msg(self, msg: String) -> LevelResult<T, StdIOError>;
}

pub trait IntoLevelInitErr<T> {
    fn into_lvl_init_err(self) -> LevelResult<T, LevelInitError>;
}

pub trait IntoLevelInsertionErr<T> {
    fn into_lvl_ins_err(self) -> LevelResult<T, LevelInsertionError>;
}

pub trait IntoLevelUpdateErr<T> {
    fn into_lvl_upd_err(self) -> LevelResult<T, LevelUpdateError>;
}

pub trait IntoLevelMapErr<T> {
    fn into_lvl_mmap_err(self) -> LevelResult<T, LevelMapError>;
}

pub trait IntoLevelExpErr<T> {
    fn into_lvl_exp_err(self) -> LevelResult<T, LevelExpansionError>;
}

impl<T> IntoLevelIOErr<T> for LevelResult<T, std::io::Error> {
    fn into_lvl_io_err(self) -> Result<T, StdIOError> {
        self.map_err(|e| StdIOError::from(e))
    }

    fn into_lvl_io_e_msg(self, msg: String) -> Result<T, StdIOError> {
        self.map_err(|e| StdIOError::with_message(msg, e))
    }
}

impl<T> IntoLevelInitErr<T> for LevelResult<T, StdIOError> {
    fn into_lvl_init_err(self) -> LevelResult<T, LevelInitError> {
        self.map_err(|e| LevelInitError::from(e))
    }
}

impl<T> IntoLevelInitErr<T> for LevelResult<T, LevelMapError> {
    fn into_lvl_init_err(self) -> LevelResult<T, LevelInitError> {
        self.map_err(|e| LevelInitError::from(e))
    }
}

impl<T> IntoLevelInsertionErr<T> for LevelResult<T, LevelMapError> {
    fn into_lvl_ins_err(self) -> LevelResult<T, LevelInsertionError> {
        self.map_err(|e| LevelInsertionError::from(e))
    }
}

impl<T> IntoLevelUpdateErr<T> for LevelResult<T, LevelInsertionError> {
    fn into_lvl_upd_err(self) -> LevelResult<T, LevelUpdateError> {
        self.map_err(|e| LevelUpdateError::from(e))
    }
}

impl<T> IntoLevelMapErr<T> for LevelResult<T, StdIOError> {
    fn into_lvl_mmap_err(self) -> LevelResult<T, LevelMapError> {
        self.map_err(|e| LevelMapError::from(e))
    }
}

impl<T> IntoLevelExpErr<T> for LevelResult<T, LevelMapError> {
    fn into_lvl_exp_err(self) -> LevelResult<T, LevelExpansionError> {
        self.map_err(|e| LevelExpansionError::from(e))
    }
}

macro_rules! map_err {
    ($src_err:ident, $dst_err:ident::$dst_var:ident) => {
        impl From<$src_err> for $dst_err {
            fn from(value: $src_err) -> Self {
                $dst_err::$dst_var(value)
            }
        }
    };
}

map_err!(StdIOError, LevelInitError::IOError);
map_err!(StdIOError, LevelMapError::IOError);

map_err!(LevelMapError, LevelInitError::MmapError);
map_err!(LevelMapError, LevelInsertionError::MmapError);
map_err!(LevelMapError, LevelExpansionError::MmapError);

map_err!(LevelInsertionError, LevelUpdateError::InsertionErr);

map_err!(LevelUpdateError, LevelExpansionError::UpdateError);

impl From<std::io::Error> for StdIOError {
    fn from(value: std::io::Error) -> Self {
        return Self::new(None, value);
    }
}

impl From<LevelInsertionError> for LevelExpansionError {
    fn from(value: LevelInsertionError) -> Self {
        LevelExpansionError::UpdateError(LevelUpdateError::from(value))
    }
}
