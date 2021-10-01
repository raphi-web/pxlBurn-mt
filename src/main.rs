extern crate crossbeam;
extern crate geo;
extern crate geo_types;
extern crate geojson;
extern crate indicatif;
extern crate linya;
extern crate num_cpus;
extern crate structopt;
use std::sync::{Arc, Mutex};

use geo::algorithm::intersects::Intersects;
use geojson::GeoJson;

use gdal::raster::{Buffer, RasterBand};
use gdal::Dataset;

use std::convert::TryInto;

use std::path::PathBuf;
use std::str::FromStr;

use std::fs;
use std::thread::{self, JoinHandle};
use structopt::StructOpt;

#[derive(StructOpt)]
#[structopt(name = "basic")]
struct Cli {
    #[structopt(parse(from_os_str))]
    json_path: PathBuf,

    #[structopt(parse(from_os_str))]
    raster_path: PathBuf,

    #[structopt(parse(from_os_str))]
    output_raster: PathBuf,

    #[structopt(default_value = "1", long, short = "v")]
    burn_value: u8,

    #[structopt(long, short = "z")]
    set_zero: bool,
}

fn main() {
    // access command line arguments
    let args = Cli::from_args();
    let input_geojson = args.json_path.as_path();
    let input_raster = args.raster_path.as_path();
    let output_raster = args.output_raster.into_os_string().into_string().unwrap();

    let burn_value = args.burn_value;
    let set_zero = args.set_zero;

    // get the data of the input raster
    let raster_dataset = Dataset::open(input_raster).expect("Error opening raster file");

    let bounds = raster_dataset.geo_transform().unwrap();
    let projection = raster_dataset.projection();
    let rasterband: RasterBand = raster_dataset
        .rasterband(1)
        .expect("Error: Raster-Band could not be read");
    let cols = rasterband.x_size();
    let rows = rasterband.y_size();

    // upper left & resolution
    let (ul_left, xres, _, ul_top, _, _yres) = (
        bounds[0], bounds[1], bounds[2], bounds[3], bounds[4], bounds[5],
    );

    let size: i64 = (rows * cols) as i64;
    let mut rast: Vec<u32> = vec![0; size as usize];
    let rast = &mut rast[..];

    // read rasterband into the vector named "rast"
    rasterband
        .read_into_slice(
            (0, 0),
            (cols as usize, rows as usize),
            (cols as usize, rows as usize),
            rast,
            None,
        )
        .expect("Error reading Raster File");

    // read the geojson
    let geojson_str =
        fs::read_to_string(input_geojson).expect("Something went wrong reading the GeoJson");

    let geojson = GeoJson::from_str(&geojson_str).expect("Error: Could not decode GeoJson");
    let geom: geo_types::Geometry<f64> = geojson.try_into().unwrap();

    // iterate over the pixels of the raster

    let num_threads = num_cpus::get() - 2;
    let mut threads: Vec<JoinHandle<Vec<u32>>> = Vec::new();

    let rast_vec: Vec<u32> = Vec::from(rast);

    let progress = Arc::new(Mutex::new(linya::Progress::new()));

    let mut thread_count = 0;
    for (start, end) in split_rows(rows, num_threads) {
        let raster_slice: Vec<u32> = rast_vec[start * cols..end * cols].into();

        let mut rs = raster_slice.clone();
        let bw: u32 = burn_value.into();
        let bw = bw.clone();
        let sz = set_zero.clone();

        let features = geom.clone();
        let progress_child = progress.clone();
        thread_count = thread_count + 1;

        let t = thread::spawn(move || {
            let mut ii = 0.clone();

            let bar: linya::Bar = progress_child
                .lock()
                .unwrap()
                .bar((rs.len() - 1) as usize, format!("Thread {}", thread_count));

            for i in start..end {
                for j in 0..cols {
                    // get the pixel coordinates in longitude & latitude
                    let (px_x, px_y) = get_coordinates(i, j, xres, ul_left, ul_top);
                    let left = px_x - (xres / 2.);
                    let right = px_x + (xres / 2.);
                    let bottom = px_y - (xres / 2.);
                    let top = px_y + (xres / 2.);

                    // create polygon based on pixel
                    let polygon = geo::Polygon::new(
                        geo_types::LineString::from(vec![
                            (left, bottom),
                            (left, top),
                            (right, top),
                            (right, bottom),
                            (left, bottom),
                        ]),
                        vec![],
                    );

                    if polygon.intersects(&features) {
                        rs[ii] = bw;
                    } else if sz {
                        rs[ii] = 0
                    }

                    ii += 1;
                    if ii % 1000 == 0 {
                        progress_child.lock().unwrap().set_and_draw(&bar, ii);
                    }
                }
            }

            return rs;
        });

        threads.push(t);
    }
    let mut compute_result: Vec<u32> = vec![];
    for t in threads {
        let mut value = t.join().unwrap();

        compute_result.append(&mut value);
    }

    // declare the driver eg. file type
    let driver = gdal::Driver::get("GTiff").unwrap();

    // create output file
    let mut dataset = driver
        .create_with_band_type::<u32>(&output_raster, cols as isize, rows as isize, 1)
        .expect("Could not create output raster");

    // set the geometry parameters
    dataset
        .set_projection(&projection)
        .expect("Error setting Projection");
    dataset
        .set_geo_transform(&bounds)
        .expect("Error setting Geo-Transform");

    // create buffer and write butter to file
    let mut rb = dataset.rasterband(1).unwrap();

    let buff: Buffer<u32> = Buffer {
        size: (cols, rows),
        data: compute_result,
    };

    rb.write((0, 0), (cols, rows), &buff)
        .expect("Error writing new Raster to band");
}

// gets the coordinates of a pixel in longitude & latitude
fn get_coordinates(row: usize, col: usize, resolution: f64, left: f64, top: f64) -> (f64, f64) {
    let x = left + (resolution / 2.0) + ((col as f64) * resolution);
    let y = top - (resolution / 2.0) - ((row as f64) * resolution);
    return (x, y);
}
fn split_rows(rows: usize, num_threads: usize) -> Vec<(usize, usize)> {
    let mut i = 0;
    let mut result: Vec<(usize, usize)> = Vec::new();

    if num_threads >= rows {
        for _ in 0..rows {
            let start_idx = i;
            let end_idx = i + 1;
            result.push((start_idx, end_idx));
            i = i + 1;
        }
    } else {
        let interval: usize = (rows as f32 / num_threads as f32).ceil() as usize;

        for _ in 0..num_threads {
            let start_idx = i;
            let mut end_idx = i + interval;

            let mut break_trigger = false;

            if end_idx >= rows {
                end_idx = rows;
                break_trigger = true;
            }

            result.push((start_idx as usize, end_idx as usize));
            i = i + interval;

            if break_trigger {
                break;
            }
        }
    }

    return result;
}
