# Video Chunker

```bash
curl -i -X POST http://localhost:8000/chunk-video \
    -F "video=@/Users/antonioferegrino/Downloads/_empuram.mp4;type=video/mp4"
```

Create the smallest video:

```bash
ffmpeg -f lavfi -i color=c=black:s=16x16:d=1 -c:v libx264 -crf 51 -preset veryslow -tune stillimage -movflags +faststart -pix_fmt yuv420p smallest.mp4
```
