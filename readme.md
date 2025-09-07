# Movie insights pipeline

## Test

```bash
uv run --directory tests test_full_system.py
```

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