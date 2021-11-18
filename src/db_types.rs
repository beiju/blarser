use diesel_derive_enum::DbEnum;

#[derive(DbEnum, Debug)]
#[DieselType = "Log_type"]
pub enum LogType {
    Debug,
    Info,
    Warning,
    Error,
}