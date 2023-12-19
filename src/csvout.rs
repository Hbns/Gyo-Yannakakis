use arrow::{csv::Writer, record_batch::RecordBatch};

use std::{fs::File, io::BufWriter};

// write to csv via arrow_csv::writer
pub fn write_record_batch_to_csv(record_batch: &RecordBatch, filename: &str) {
    //let filename = "rb2csv.csv";

    // Create a file and wrap it with a buffered writer
    let file = File::create(filename).expect("Unable to create file");
    let buffered_file = BufWriter::new(file);

    let mut writer = Writer::new(buffered_file);
    writer.write(record_batch);
}