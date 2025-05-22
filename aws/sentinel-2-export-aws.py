from pystac_client import Client
import json
import boto3
from urllib.parse import urlparse
import stackstac
import rasterio
from rasterio.session import AWSSession
import rioxarray
import geopandas as gpd



def run_query(aoi_bounds, start_time, end_time, collection_name):
    """Run query to get Sentinel-2 scenes from STAC catalog."""

    # STAC Catalog URL to search for scenes.
    catalog = Client.open("https://earth-search.aws.element84.com/v1/")

    # Search for items instersecting our bbox and within desired date range.
    query = catalog.search(
        collections=[collection_name],
        datetime=f"{start_time}/{end_time}",
        bbox=aoi_bounds
    )
    
    # Get the items (i.e. Sentinel-2 scenes) that match the query.
    items = query.item_collection()

    return items



# TODO: should this be saved locally? be stored in memory and copied directly to S3? 
def export_local_metadata_json(item, out_path):
    """Export metadata json from each item to a local file."""
    with open(out_path, 'w') as f:
        item_dict = item.to_dict()
        json.dump(item_dict, f, indent=2)
        print(f"Metadata JSON saved to {out_path}")
    return


def stack_item(item, epsg_code, aoi_bounds, bands_of_interest):

    stack = stackstac.stack(
        item,
        epsg=epsg_code,
        bounds_latlon=aoi_bounds,
        chunksize=1024
    )
    print(f"Initial stack data shape: {stack.shape}")

    stack = stack.sel(band=bands_of_interest)

    # Check we only have one time period
    if len(stack.time) > 1:
        raise ValueError("More than one time period found")
    
    else:
        stack = stack.squeeze("time")

    # Check shape of stacked data:
    print(f"Final stac data shape: {stack.shape}")

    return stack


def upload_stack_to_s3(stack, destination_bucket, prefix, scene_id, filename):
    """
    Uploads a stack of Sentinel-2 data to S3, using a similar approach to the
    successful land cover function.
    """
    # Extract metadata from the stacked data
    transform, crs = stack.rio.transform(), stack.rio.crs
    height, width = stack.sizes["y"], stack.sizes["x"]
    count = stack.sizes["band"]  # Number of bands
    dtype = stack.dtype

    print(f"Uploading {filename} to S3 with dimensions: {height} x {width} x {count}")
    
    # Get band names for descriptions
    band_names = stack.band.values
    print(f"Band names: {band_names}")

    # Initialize boto3 and rasterio AWS session
    boto3_session = boto3.Session(profile_name='default')
    aws_session = AWSSession(boto3_session)

    # Define path to upload the stacked data to
    s3_path = f"s3://{destination_bucket}/{prefix}{scene_id}/{filename}"

    # Set gdal config
    gdal_config = {
        'CPL_VSIL_USE_TEMP_FILE_FOR_RANDOM_WRITE': 'YES',  
        'AWS_REGION': 'us-west-2',  # Set this to your bucket's region
        'AWS_S3_ENDPOINT': 's3.amazonaws.com'
    }

    # Upload the stacked data to S3
    with rasterio.Env(aws_session, **gdal_config):
        with rasterio.open(
            s3_path,
            "w",
            driver="GTiff",
            height=height,
            width=width,
            count=count,
            dtype=dtype,
            crs=crs,
            transform=transform,
        ) as dst:
            # Simply iterate directly through the band dimension
            # This follows the same pattern as your land cover function
            for i, (band_name, band_data) in enumerate(zip(band_names, stack)):

                # Write the band data (adding 1 because rasterio bands are 1-indexed)
                dst.write(band_data.values, i + 1)
                
                # Set the band description to the band name
                dst.set_band_description(i + 1, str(band_name))

    print(f"Successfully uploaded to {s3_path}")
    return s3_path



def main(bbox, start_time, end_time, collection_name, bands_of_interest, destination_bucket, prefix):
    """Get Sentinel-2 data for the specified parameters and copy to S3 bucket."""

    # Query the data 
    items = run_query(bbox, start_time, end_time, collection_name)
    print(f"Found {len(items)} matching items.")

    # Loop through each item and copy the bands of interest to S3.
    for item in items:
        scene_id = item.id
        print(f"Processing scene: {scene_id}")

        # Stack the data
        epsg_code = int(item.properties['proj:code'].split(":")[-1]) # EPSG code as int (e.g. 4326)
        stack = stack_item(item, epsg_code, bbox, bands_of_interest)
        
        # Export metadata JSON for the item - TODO: copy this to S3 as well
        metadata_json_path = f"{scene_id}_metadata.json"
        export_local_metadata_json(item, metadata_json_path)

        # Upload metadata JSON to S3
        s3_client = boto3.client('s3')
        metadata_s3_key = f"{prefix}{scene_id}/stac_metadata.json"
        s3_client.upload_file(metadata_json_path, destination_bucket, metadata_s3_key)
        print(f"Uploaded metadata to s3://{destination_bucket}/{metadata_s3_key}")
        
        # Upload the stacked scene data to S3
        filename = f"{scene_id}.tif"
        stack_path = upload_stack_to_s3(stack, destination_bucket, prefix, scene_id, filename)
        print(f"Uploaded {scene_id} to {stack_path}")
        
    
    print("All items processed!")

    return


# Example usage
if __name__ == "__main__":

    # Get bounds from the aoi
    geojson_path = "aois/guatemala_aoi.geojson"
    aoi = gpd.read_file(geojson_path)
    aoi = aoi.to_crs("EPSG:4326")
    bbox = aoi.geometry.values[0].bounds
    print(f"AOI bounds: {bbox}")



    start_time = "2023-06-01"
    end_time = "2023-06-10"
    collection_name = "sentinel-2-l2a"


    bands_of_interest = ['aot', 'blue', 'coastal', 'green', 'nir', 'nir08', 'nir09', 'red', 'rededge1', 
                        'rededge2', 'rededge3', 'swir16', 'swir22', 'wvp', 'scl']

    destination_bucket = 'cs-awsopendata-sentinel2'

    prefix = 'guatemala-june-2023/'

    # Call the function to get Sentinel-2 data and copy to S3.
    main(bbox, start_time, end_time, collection_name, bands_of_interest, destination_bucket, prefix)


