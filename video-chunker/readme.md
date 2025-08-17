# Video Chunker

## Utility scripts

Create a small video:

```bash
ffmpeg -f lavfi -i color=c=black:s=16x16:d=1 -c:v libx264 -crf 51 -preset veryslow -tune stillimage -movflags +faststart -pix_fmt yuv420p ./tests/fixtures/smallest.mp4
```

Upload the video to the chunker service:

```bash
curl -i -X POST http://localhost:8000/chunk-video \
    -F "video=@$PWD/tests/fixtures/smallest.mp4;type=video/mp4"
```
