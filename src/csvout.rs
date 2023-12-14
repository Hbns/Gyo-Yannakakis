use std::{fs::File, error::Error};
use csv::Writer;

pub fn write_to_csv(data: &Vec<Vec<&str>>) -> Result<(), Box<dyn Error>> {
    // Specify the path to the CSV file
    let csv_file_path = "output.csv";

    // Open the file in write mode
    let file = File::create(csv_file_path)?;

    // Create a CSV writer
    let mut csv_writer = Writer::from_writer(file);

    // Write the data to the CSV file
    for row in data {
        csv_writer.write_record(row)?;
    }

    // Flush the writer to make sure all data is written to the file
    csv_writer.flush()?;

    println!("CSV file successfully created at: {}", csv_file_path);

    Ok(())
}