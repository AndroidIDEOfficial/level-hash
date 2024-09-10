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

macro_rules! log_trace {
    ($($arg:tt)*) => {
        crate::log_macros::do_log!("TRACE", $($arg)*)
    };
}

macro_rules! log_debug {
    ($($arg:tt)*) => {
        crate::log_macros::do_log!("DEBUG", $($arg)*)
    };
}

macro_rules! log_info {
    ($($arg:tt)*) => {
        crate::log_macros::do_log!("INFO", $($arg)*)
    };
}

macro_rules! log_warn {
    ($($arg:tt)*) => {
        crate::log_macros::do_log!("WARN", $($arg)*)
    };
}

macro_rules! log_error {
    ($($arg:tt)*) => {
        crate::log_macros::do_log!("ERROR", $($arg)*)
    };
}

macro_rules! do_log {
    ($level:literal, $($arg:tt)*) => {
        println!("[{}] [{}:{}:{}] {}", $level, module_path!(), file!(), line!(), format_args!($($arg)*))
    };
}

pub(crate) use do_log;
pub(crate) use log_debug;
pub(crate) use log_error;
pub(crate) use log_info;
pub(crate) use log_trace;
pub(crate) use log_warn;
