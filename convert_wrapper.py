import os

import torch
from model import MattingNetwork
from inference_yield import convert_video

model = MattingNetwork('mobilenetv3').eval()#.cuda()  # or "resnet50"
model.load_state_dict(torch.load('rvm_mobilenetv3.pth'))

source_projects_folder = "content_projects"
project_name = "art"
out_projects_folder = os.path.join("static", "content_projects_output")

path = os.path.join(out_projects_folder, project_name)

if not os.path.exists(path):
    os.makedirs(path)

input_source_filename = "art.mp4"
output_composition_filename = "art_out.mp4"

input_source_filename_path = os.path.join(source_projects_folder, project_name, input_source_filename)

convert_video(
    model,  # The model, can be on any device (cpu or cuda).
    input_source=input_source_filename_path,  # A video file or an image sequence directory.
    output_type='video',  # Choose "video" or "png_sequence"
    output_composition='output.mp4',  # File path if video; directory path if png sequence.
    output_video_mbps=4,  # Output video mbps. Not needed for png sequence.
    downsample_ratio=None,  # A hyperparameter to adjust or use None for auto.
    seq_chunk=12,  # Process n frames at once for better parallelism.
)
