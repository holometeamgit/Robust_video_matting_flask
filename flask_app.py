import os

import flask
from flask import Flask, stream_with_context
from flask import render_template, request

from inference_yield import convert_video

app = Flask(__name__, static_url_path='/static')


@app.route("/")
def hello():
    return "<h1 style='color:blue'>Welcome to </br>AWESOME BEEM SERVER!</h1>"


@app.route('/upload')
def upload_file_page():
    return render_template('upload.html')


def stream_template(template_name, **context):
    app.update_template_context(context)
    t = app.jinja_env.get_template(template_name)
    rv = t.stream(context)
    rv.disable_buffering()
    return rv


@app.route('/uploader', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        # get project and create directory for it
        project_name = request.form['project']
        project_dir = os.path.join("content_projects", project_name)
        if not os.path.exists(project_dir):
            os.makedirs(project_dir)

        # get video input video file
        f1 = request.files['video_file']
        complete_name1 = os.path.join(project_dir, "input.mp4")
        f1.save(complete_name1)

        output_dir_var = os.path.join(os.path.join("static", "content_projects_output"), project_name)
        server_uri_var = request.base_url[:-8]

        # run bg matting process
        result = convert_video(input_source=complete_name1,      # input file path WITH file name
                               output_dir=output_dir_var,        # output file path WITHOUT file name
                               output_type='video',              # Choose only "video"
                               output_composition='output.mp4',  # output file name
                               output_video_mbps=4,              # Output video mbps.
                               downsample_ratio=None,            # A hyperparameter to adjust or use None for auto.
                               seq_chunk=1,                      # Frames chunk
                               server_uri=server_uri_var,        # current server uri, required for full output link
                               generate_seg_video=True)          # required to save video for ML Dataset

        # return stream with data from process method (frames count and url for file with result then)
        return flask.Response(stream_with_context(stream_template('template.html', rows=result)))


if __name__ == "__main__":
    app.run(host='0.0.0.0', threaded=True)
