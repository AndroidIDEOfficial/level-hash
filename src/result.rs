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

use crate::LevelHash;

/// An I/O error in level hash.
#[derive(Debug)]
pub struct LevelIOError {
    message: Option<String>,
    error: std::io::Error,
}

impl LevelIOError {

    /// Create a new IO error with the given message.
    pub fn with_message(message: String, error: std::io::Error) -> Self {
        Self::new(Some(message), error)
    }

    /// Create a new IO error with an optional message.
    pub fn new(message: Option<String>, error: std::io::Error) -> Self {
        LevelIOError {
            message,
            error
        }
    }
}

/// Level hash error enumeration.
#[derive(Debug)]
pub enum LevelInitError {

    /// An error caused due to IO operations.
    IOError(LevelIOError),

    /// An error caused due to invalid arguments.
    InvalidArg(String)
}

/// Result of any operation in level hash.
pub type LevelResult<T, E> = Result<T, E>;

/// Result of initializing a level hash instance.
pub type LevelInitResult = Result<LevelHash, LevelInitError>;

pub trait IntoLevelIOErr<T> {
    fn into_lioe(self) -> Result<T, LevelIOError>;
    fn into_lioe_msg(self, msg: String) -> LevelResult<T, LevelIOError>;
}

pub trait IntoLevelInitErr<T> {
    fn into_lie(self) -> LevelResult<T, LevelInitError>;
}

impl <T> IntoLevelIOErr<T> for Result<T, std::io::Error> {
    fn into_lioe(self) -> Result<T, LevelIOError> {
        self.map_err(|e| LevelIOError::from(e))
    }
    
    fn into_lioe_msg(self, msg: String) -> Result<T, LevelIOError> {
        self.map_err(|e| LevelIOError::with_message(msg, e))
    }
}

impl <T> IntoLevelInitErr<T> for Result<T, LevelIOError> {
    fn into_lie(self) -> LevelResult<T, LevelInitError> {
        self.map_err(|e| LevelInitError::IOError(e))
    }
}

impl From<std::io::Error> for LevelIOError {

    fn from(value: std::io::Error) -> Self {
        return Self::new(None, value);
    }
}