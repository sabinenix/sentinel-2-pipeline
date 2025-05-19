from pystac_client import Client
import json
import boto3
from urllib.parse import urlparse



def run_query(bbox, start_time, end_time, collection_name):
    """Run query to get Sentinel-2 scenes from STAC catalog."""

    # STAC Catalog URL to search for scenes.
    catalog = Client.open("https://earth-search.aws.element84.com/v1/")

    # Search for items instersecting our bbox and within desired date range.
    query = catalog.search(
        collections=[collection_name],
        datetime=f"{start_time}/{end_time}",
        bbox=bbox
    )
    
    # Get the items (i.e. Sentinel-2 scenes) that match the query.
    items = query.item_collection()

    return items



# TODO: should this be saved locally? be stored in memory and copied directly to S3? 
def export_metadata_json(item, out_path):
    """Export metadata json from each item to a local file."""
    with open(out_path, 'w') as f:
        item_dict = item.to_dict()
        json.dump(item_dict, f, indent=2)
        print(f"Metadata JSON saved to {out_path}")
    return



def s3_to_s3_copy(source_url, destination_bucket='cs-awsopendata-sentinel2', prefix=''):
    """Copy Sentinel-2 scene from source URL to destination S3 bucket."""

    # Parse the source URL to extract bucket and key.
    parsed_url = urlparse(source_url)
    
    # The source_url is in this format: https://sentinel-cogs.s3.us-west-2.amazonaws.com/sentinel-s2-l2a-cogs/52/L/HL/2023/1/S2B_52LHL_20230106_0_L2A/B02.tif
    url_parts = parsed_url.netloc.split('.') 
    
    # Source bucket should be 'sentinel-cogs' - the first part source url
    source_bucket = url_parts[0]
    
    # Source key is everything after the slash - e.g. 'sentinel-s2-l2a-cogs/52/L/HL/2023/1/S2B_52LHL_20230106_0_L2A/B02.tif'
    source_key = parsed_url.path.lstrip('/')

    # Construct destination key to match the source key. NOTE - can adjust destination_key as necessary.
    destination_key = f"{prefix}{source_key}"
    
    print(f"Copying from bucket: {source_bucket}, key: {source_key}")
    print(f"To bucket: {destination_bucket}, key: {destination_key}")
    
    # Initialize S3 client (region of the sentinel-cog bucket is us-west-2)
    s3 = boto3.client('s3', region_name='us-west-2')  # Specify region if needed
    
    try:
        response = s3.copy_object(
            CopySource={'Bucket': source_bucket, 'Key': source_key},
            Bucket=destination_bucket,
            Key=destination_key,
            RequestPayer='requester'
        )
        print(f"Copy successful: {response}")
        return destination_key
    
    except Exception as e:
        print(f"Error copying object: {e}")
        return None
    

def get_sentinel2_data(bbox, start_time, end_time, collection_name, bands_of_interest, destination_bucket, prefix):
    """Get Sentinel-2 data for the specified parameters and copy to S3 bucket."""

    items = run_query(bbox, start_time, end_time, collection_name)
    print(f"Found {len(items)} matching items.")

    # Loop through each item and copy the bands of interest to S3.
    for item in items:
        scene_id = item.id
        print(f"Processing scene: {scene_id}")
        
        # Export metadata JSON for the item - TODO: copy this to S3 as well
        metadata_json_path = f"{scene_id}_metadata.json"
        export_metadata_json(item, metadata_json_path)
        
        for band in bands_of_interest:
            if band in item.assets:
                asset = item.assets[band]
                source_url = asset.href
                
                print(f"Copying {band} band from {source_url}")
                destination_key = s3_to_s3_copy(source_url, destination_bucket, prefix)
                
                if destination_key:
                    print(f"Successfully copied to s3://{destination_bucket}/{destination_key}")
                else:
                    print(f"Failed to copy {band} band from {scene_id}")
    
    print("All items processed!")

    return


# Example usage
if __name__ == "__main__":
    
    # Set data request parameters.
    bbox = [132.52934211276073, -12.730063400794094, 132.54027735328083, -12.721072673008706] # Kakadu National Park
    start_time = "2023-06-01"
    end_time = "2023-06-07"
    collection_name = "sentinel-2-l2a"


    bands_of_interest = ['aot', 'blue', 'coastal', 'green', 'nir', 'nir08', 'nir09', 'red', 'rededge1', 
                        'rededge2', 'rededge3', 'swir16', 'swir22', 'wvp', 'scl', 'visual']

    destination_bucket = 'cs-awsopendata-sentinel2'

    prefix = 'kakadu-june/'

    # Call the function to get Sentinel-2 data and copy to S3.
    get_sentinel2_data(bbox, start_time, end_time, collection_name, bands_of_interest, destination_bucket, prefix)


