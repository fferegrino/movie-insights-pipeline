# Movie insights pipeline

## What is included

### Docker Compose Setup (`docker-compose.yml`)

- **MinIO Server**: A local S3-compatible storage server that runs on your machine

  - **Bucket Initialization**: Automatically creates storage bucket when you start the services

  - **Web Interface**: A browser-based console to manage files and buckets

- **Kafka**: A message broker that allows you to send and receive messages between different services

  - **Topic Initialization**: Automatically creates Kafka topics when you start the services

  - **Kafka UI**: A web interface to manage Kafka topics and messages

- **Video Chunker**: A service that chunks videos into smaller chunks

## Quick Start

```bash
docker-compose up -d
```

### Upload a video

```bash
curl -i -X POST http://localhost:8000/chunk-video \
    -F "video=@$PWD/movies/pizza-conversation.mp4;type=video/mp4"
```

### Stopping

```bash
docker-compose down
```

### To completely remove everything (including your data):

```bash
docker-compose down -v
```

## Video attributions

- Portrait of a woman eating pizza at lunch | https://mixkit.co/free-stock-video/portrait-of-a-woman-eating-pizza-at-lunch-44000/
- Chef doing acrobatics with the dough of a pizza | https://mixkit.co/free-stock-video/chef-doing-acrobatics-with-the-dough-of-a-pizza-42472/
- Girl eating and chatting with another person | https://mixkit.co/free-stock-video/girl-eating-and-chatting-with-another-person-44022/
- Portrait of a man eating pizza at lunch | https://mixkit.co/free-stock-video/portrait-of-a-man-eating-pizza-at-lunch-43999/
- Lunch between a man and a woman | https://mixkit.co/free-stock-video/lunch-between-a-man-and-a-woman-43998/
- Two young men on the bed playing video games | https://mixkit.co/free-stock-video/two-young-men-on-the-bed-playing-video-games-13137/
- Friends celebrate sports victory on sofa with pizza | https://mixkit.co/free-stock-video/friends-celebrate-sports-victory-on-sofa-with-pizza-5669/
- Friends show fear while watching movie on sofa with pizza |https://mixkit.co/free-stock-video/friends-show-fear-while-watching-movie-on-sofa-with-pizza-5709/