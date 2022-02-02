use diesel_derive_enum::DbEnum;
use serde::Serialize;

#[derive(DbEnum, Debug, Serialize)]
#[DieselType = "Log_type"]
pub enum LogType {
    Debug,
    Info,
    Warning,
    Error,
}