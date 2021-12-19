import ffmpeg
import os

def ext_a_to_v(video_source, audio_source, av_result):
    """
    Args:
    video_source: video source with audio
    audio_source: video WITHOUT audio
    av_result: result of adding audio to video
    """

    stream_in = ffmpeg.input(video_source)
    audio_in = stream_in.audio

    stream_out = ffmpeg.input(audio_source)

    out = ffmpeg.output(stream_out, audio_in, av_result, **{'q:v': 0})
    out.run() 