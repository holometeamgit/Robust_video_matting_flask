import ffmpeg
import os

def has_audio_streams(file_path):
    streams = ffmpeg.probe(file_path)["streams"]
    for stream in streams:
        if stream["codec_type"] == "audio":
            return True
    return False

def video_correction(video_source, audio_source, rotation, av_result):
    """
    Add audio to video and fix orientation if it was recorded in metadata
    Args:
       video_source: video source with audio
       audio_source: video WITHOUT audio
       rotation: rotation data from video metadata. Right now it works only for 90 and 270 degrees.
       av_result: result of adding audio to video
    """

    stream_in = ffmpeg.input(video_source)

    is_audio_streams_exist = has_audio_streams(video_source)
    print("is_audio_streams_exist:" + str(is_audio_streams_exist))

    audio_in = stream_in.audio

    stream_out = ffmpeg.input(audio_source)

    video_out = stream_out.video
    if rotation is not None:
        if rotation == 90:
            video_out = stream_out.video.filter("transpose", 1)
        if rotation == 270:
            video_out = stream_out.video.filter("transpose", 2)
        # if rotation == 180:  # there were no examples of such videos yet
        #     video_out = stream_out.video.filter("transpose", 2).filter("transpose", 2)

    out = None
    if is_audio_streams_exist is True:
        out = ffmpeg.output(video_out, audio_in, av_result, **{'q:v': 0})
    else:
        out = ffmpeg.output(video_out, av_result, **{'q:v': 0})

    out.global_args('-y').run()
