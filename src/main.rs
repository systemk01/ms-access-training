//! A program executing a query and printing the result as csv to standard out. Requires
//! `anyhow` and `csv` crate.

use anyhow::Error;
use odbc_api::{buffers::TextRowSet, parameter::{Blob,BlobSlice}, IntoParameter, sys::Date, Connection, ConnectionOptions, Cursor, Environment, ResultSetMetadata};
use std::{
    ffi::CStr,
    io::{stdout, Write},
    path::PathBuf,
};

/// Maximum number of rows fetched with one row set. Fetching batches of rows is usually much
/// faster than fetching individual rows.
const BATCH_SIZE: usize = 5000;

fn main() -> Result<(), Error> {
    // Write csv to standard out
    let out = stdout();
    let mut writer = csv::Writer::from_writer(out);

    let database_path = r"C:\Users\khg\Documents\RustProjects\msaccess-learn\MSAccess1.accdb";
    let environment = Environment::new()?;

    let connection_string = format!("Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={}", database_path);

    // Connect using a DSN. Alternatively we could have used a connection string
    let connection = environment.connect_with_connection_string(&connection_string, ConnectionOptions::default())?;
    
    let birthday:Date = Date{year:2024,month:12,day:15};
    let _ = insert_user(&connection, 12, "Mikey", birthday);

    // Execute a one of query without any parameters.
    match connection.execute("SELECT * FROM tab_users", ())? {
        Some(mut cursor) => {
            // Write the column names to stdout
            let headline : Vec<String> = cursor.column_names()?.collect::<Result<_,_>>()?;
            writer.write_record(headline)?;

            // Use schema in cursor to initialize a text buffer large enough to hold the largest
            // possible strings for each column up to an upper limit of 4KiB.
            let mut buffers = TextRowSet::for_cursor(BATCH_SIZE, &mut cursor, Some(4096))?;
            // Bind the buffer to the cursor. It is now being filled with every call to fetch.
            let mut row_set_cursor = cursor.bind_buffer(&mut buffers)?;

            // Iterate over batches
            while let Some(batch) = row_set_cursor.fetch()? {
                // Within a batch, iterate over every row
                for row_index in 0..batch.num_rows() {
                    // Within a row iterate over every column
                    let record = (0..batch.num_cols()).map(|col_index| {
                        batch
                            .at(col_index, row_index)
                            .unwrap_or(&[])
                    });
                    // Writes row as csv
                    writer.write_record(record)?;
                }
            }
        }
        None => {
            eprintln!(
                "Query came back empty. No output has been created."
            );
        }
    }

    Ok(())

}


fn insert_user(conn:&Connection<'_>,user_number: i32, user_name: &str, birthday: Date) -> Result<(), Error> {

    let insert = "INSERT INTO tab_users (User_Number, User_Name, Birth_Date) VALUES (?,?,?)";
    let parameters = (&user_number, &user_name.into_parameter(),  &birthday.into_parameter()); 
    conn.execute(&insert, parameters)?;
    Ok(())
}