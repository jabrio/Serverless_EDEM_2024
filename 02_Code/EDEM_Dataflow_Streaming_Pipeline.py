""" 
Script: Dataflow Streaming Pipeline

Description: This script will be responsible for processing 
messages ingested by our messaging queue from the device and:

    1. Calculate the average speed per vehicle in the section.

    2. Invoke the Vision AI model if the speed exceeds the allowed limit in the section.

    3. Finally, all the information will be sent to another topic for further analysis.

EDEM. Master Data Analytics 2024
Professor: Javi Briones
"""

""" Import libraries """

# Import Beam Libraries

import apache_beam as beam
from apache_beam.runners import DataflowRunner
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window
from apache_beam.metrics import Metrics

# Dataflow ML
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import RunInference

# Import GCP Libraries
from google.cloud.vision_v1.types import Feature
from google.cloud import vision

# Import Common Libraries
import argparse
import requests
import logging
import json
import re
import io

beam.options.pipeline_options.PipelineOptions.allow_non_parallel_instruction_output = True
DataflowRunner.__test__ = False


""" Helpful functions """
def ParsePubSubMessage(message):

    # Decode PubSub message in order to deal with it
    
    # ToDo: Complete this section
    
    # Convert string decoded in JSON format
    
    # ToDo: Complete this section

    # Return function
    
    # ToDo: Complete this section

def getVehicleImage(item,api_url):

    import requests
    import io

    # API call to simulate a photo captured by the radar
    
    # ToDo: Complete this section

    # Read the image (using io.BytesIO) from the Google Cloud Storage URL obtained in the previous step.

    # ToDo: Complete this section

    # Append image_url to the payload
    
    # ToDo: Complete this section

    # Return


class CloudVisionModelHandler(ModelHandler):

    def load_model(self):
        
        """Initiate the Google Vision API client."""

        from google.cloud import vision
        
        client = vision.ImageAnnotatorClient()
        return client
    
    def run_inference(self, batch, model, inference):

        from google.cloud import vision
        from google.cloud.vision_v1.types import Feature

        feature = Feature()
        feature.type_ = Feature.Type.TEXT_DETECTION

        images = [vision.Image(content=image_bytes) for (item, image_bytes) in batch]
        item_list = [item for (item, image_bytes) in batch]

        image_requests = [vision.AnnotateImageRequest(image=image, features=[feature]) for image in images]
        batch_image_request = vision.BatchAnnotateImagesRequest(requests=image_requests)

        model_responses = model.batch_annotate_images(request=batch_image_request).responses

        # Deal with the model's response to extract the text we need
        # ToDo: Complete this section
        
        yield output_dict, response

class OutputFormatDoFn(beam.DoFn):

    def process(self, element):

        output_dict, texts = element

        if len(texts) > 0 :

            # Set the pattern to recognize the license plate among the texts the model might find.

            # ToDo: Complete this section
            
            yield output_dict

        else:

            output_dict['license_plate'] = "no texts found"
            yield output_dict
        

# DoFn

class getVehicleDoFn(beam.DoFn):

    def process(self, element):

        # Get vehicle_id from input payload

        # ToDo: Complete this section
 

class avgSpeedDoFn(beam.DoFn):

    def __init__(self,radar_id):

        self.countFinedVehicles = Metrics.counter('main', 'Count of fined vehicles.')
        self.countNonFinedVehicles = Metrics.counter('main', 'Count of non-fined vehicles.')
        self.radar_id = radar_id

    def process(self, element):

        import apache_beam as beam
        
        key, payload = element
        # Calculate the average speed per vehicle
        avg_speed = # ToDo: Complete this section

        output_dict = {
            "radar_id": self.radar_id,
            "vehicle_id": key,
            "avg_speed": avg_speed,
            "coordinates": payload[-1]['location']
        }

        # Create two distinct PCollections, one for fined vehicles and another for those 
        # that have not been fined, so we can process the data differently.

        if avg_speed > 40:

            output_dict['is_Ticketed'] = True

            # ToDo: Complete this section
        
        else:

            output_dict['is_Ticketed'] = False
            output_dict['license_plate'] = None

            # ToDo: Complete this section


""" Dataflow Process """

def run():

    """ Input Arguments"""
    parser = argparse.ArgumentParser(description=('Arguments for the Dataflow Streaming Pipeline.'))

    parser.add_argument(
                '--project_id',
                required=True,
                help='GCP cloud project name.')
    
    parser.add_argument(
                '--input_subscription',
                required=True,
                help='PubSub subscription from which we will read data from the generator.')
    
    parser.add_argument(
                '--output_topic',
                required=True,
                help='PubSub Topic which will be the sink for our data.')

    parser.add_argument(
                '--radar_id',
                required=True,
                help="Radar ID corresponding to the student's name.")

    parser.add_argument(
                '--cars_api',
                required=True,
                help="API for retrieving vehicle images.")

    args, pipeline_opts = parser.parse_known_args()

    
    """ Apache Beam Pipeline """
    
    # Pipeline Options
    options = PipelineOptions(pipeline_opts,
        save_main_session=True, streaming=True, project=args.project_id)

    # Pipeline

    with beam.Pipeline(argv=pipeline_opts,options=options) as p:

        """ Part 01: Read data from PubSub. """

        data = (
            p
                | "Read From PubSub" >> #ToDo: Complete this section
                | "Parse JSON messages" >> #ToDo: Complete this section
        )

        """ Part 02: Get the aggregated data of the vehicle within the section. """

        processed_data = (
            
            data 
                | "Extract vehicle id data" >> #ToDo: Complete this section
                | "User-window based on each vehicle" >> #ToDo: Complete this section
                | "Group by ID" >> #ToDo: Complete this section
                | "Avg Speed" >> #ToDo: Complete this section
        
        )

        (
            processed_data.fined_vehicles
                | "Capture Vehicle image" >> # ToDo: Complete this section
                | "Model Inference" >> RunInference(model_handler=CloudVisionModelHandler())
                | "Output Format" >> #ToDo: Complete this section
                | "Encode fined_vehicles to Bytes" >> #ToDo: Complete this section
                | "Write fined_vehicles to PubSub" >> #ToDo: Complete this section
        )

        (
            processed_data.non_fined_vehicles 
                | "Encode non_fined_vehicles to Bytes" >> #ToDo: Complete this section
                | "Write non_fined_vehicles to PubSub" >> #ToDo: Complete this section
        )
        

if __name__ == '__main__':

    # Set Logs
    logging.getLogger().setLevel(logging.INFO)

    logging.info("The process started")

    # Run Process
    run()