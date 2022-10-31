use chrono::NaiveDateTime;
use csv::StringRecord;
use rusqlite::{params, Connection, Result};
use std::ffi::OsString;
use std::fs::{read_dir, DirEntry};

#[derive(Debug)]
struct Data {
    date: NaiveDateTime,
    url: String,
    host: String,
    description: String,
}
fn open_my_db() -> Result<Connection, rusqlite::Error> {
    let conn = Connection::open_in_memory()?;
    conn.execute(
        "CREATE TABLE urls (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATETIME NOT NULL,
            url  TEXT NOT NULL,
            host TEXT NOT NULL,
            description TEXT
        )",
        (), // empty list of parameters.
    )?;
    Ok(conn)
}
fn insert(conn: &Connection, x: &Data) -> Result<usize, rusqlite::Error> {
    conn.execute(
        "insert into urls (date, url, host, description) values (?1, ?2, ?3, ?4)",
        params![x.date, x.url, x.host, x.description],
    )
}
fn each_year_dir() -> impl Iterator<Item = DirEntry> {
    let paths = read_dir("phishurl-list").unwrap();
    paths.map(|x| x.unwrap()).filter(|x| {
        let path = x.path();
        path.is_dir() && !path.file_name().unwrap().to_str().unwrap().starts_with(".")
    })
}
fn each_csv() -> impl Iterator<Item = OsString> {
    each_year_dir().flat_map(|x| {
        read_dir(x.path())
            .unwrap()
            .map(|x| x.unwrap().path().into_os_string())
    })
}
fn record_to_data(record: &StringRecord) -> Result<Data, Box<dyn std::error::Error>> {
    let date = record.get(0).ok_or("no date")?;
    let url = record.get(1).ok_or("no url")?;
    let description = record.get(2).ok_or("no description")?;

    let date = NaiveDateTime::parse_from_str(date, "%Y/%m/%d %H:%M:%S")?;
    let url = url::Url::parse(url)?;
    let host = url.host_str().ok_or("no host")?;
    Ok(Data {
        date,
        url: url.to_string(),
        host: host.to_string(),
        description: description.to_string(),
    })
}
fn main() {
    let conn = open_my_db().unwrap();
    for path in each_csv() {
        let mut rdr = csv::Reader::from_path(&path).unwrap();
        for record in rdr.records() {
            match record {
                Ok(record) => match record_to_data(&record) {
                    Ok(data) => {
                        insert(&conn, &data).unwrap();
                    }
                    Err(e) => {
                        println!("{} at {:?} {:?}", e, path, record)
                    }
                },
                Err(e) => println!("{} at {:?}", e, path),
            }
        }
    }
    conn.backup(rusqlite::DatabaseName::Main, "phishurl.db3", None)
        .unwrap();
}
