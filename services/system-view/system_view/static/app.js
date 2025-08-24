class SystemViewApp {
    constructor() {
        this.ws = null;
        this.connect();
        this.initElements();
        this.loadInitialView();

        this.chunkMessages = [];
        this.sceneMessages = [];
    }

    initElements() {
        this.messageTracks = {
            chunks: document.getElementById('message-track-chunks'),
            scenes: document.getElementById('message-track-scenes'),
        };
    }

    loadInitialView() {
        fetch(`/api/topics/video-chunks/messages`)
            .then(response => response.json())
            .then(data => {
                for (const message of data.messages) {
                    this.appendChunkMessage(message.value);
                }
            });

        fetch(`/api/topics/scenes/messages`)
            .then(response => response.json())
            .then(data => {
                for (const message of data.messages) {
                    this.appendSceneMessage(message.value);
                }
            });
    }

    appendChunkMessage(chunkMessage) {
        this.chunkMessages.push(chunkMessage);
        this.renderChunkMessages();
    }

    appendSceneMessage(sceneMessage) {
        this.sceneMessages.push(sceneMessage);
        this.renderSceneMessages();
    }

    renderChunkMessages() {
        const template = document.getElementById('chunk-template').innerHTML;
        this.messageTracks.chunks.innerHTML = "";
        this.chunkMessages.sort((a, b) => a.start_ts - b.start_ts);
        for (const chunkMessage of this.chunkMessages) {
            const rendered = Mustache.render(template, chunkMessage);
            this.messageTracks.chunks.innerHTML += rendered;
        }
    }

    renderSceneMessages() {
        const template = document.getElementById('scene-template').innerHTML;
        this.messageTracks.scenes.innerHTML = "";

        const scenes = {}

        for (const sceneMessage of this.sceneMessages) {
            if (!scenes[sceneMessage.scene_id]) {
                scenes[sceneMessage.scene_id] = {
                    id: sceneMessage.scene_id,
                    messages: []
                }
            }
            scenes[sceneMessage.scene_id].messages.push(sceneMessage);
        }

        const sceneList = []

        for (const sceneId in scenes) {
            console.log(scenes[sceneId]);
            let start_ts = 1000000
            let end_ts = -1000000
            let individual_scenes = []
            for (const message of scenes[sceneId].messages) {
                if (message.video_start_time < start_ts) {
                    start_ts = message.video_start_time
                }
                if (message.video_end_time > end_ts) {
                    end_ts = message.video_end_time
                }
                individual_scenes.push({
                    chunk_id: message.chunk_id,
                    start_time: message.start_time,
                    end_time: message.end_time
                })
            }

            individual_scenes.sort((a, b) => a.start_time - b.start_time);
            sceneList.push({
                id: sceneId,
                start_ts: start_ts,
                end_ts: end_ts,
                individual_scenes: individual_scenes
            })
        }

        sceneList.sort((a, b) => a.start_ts - b.start_ts);
        for (const scene of sceneList) {
            const rendered = Mustache.render(template, scene);
            this.messageTracks.scenes.innerHTML += rendered;
        }
    }

    connect() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/ws`;
        this.ws = new WebSocket(wsUrl);


        try {
            this.ws = new WebSocket(wsUrl);

            this.ws.onopen = () => {
                console.log('WebSocket connected');
            };

            this.ws.onmessage = (event) => {
                // console.log('WebSocket message received:', event.data);
            };

            this.ws.onclose = () => {
                console.log('WebSocket disconnected');
            };

            this.ws.onerror = (error) => {
                console.error('WebSocket error:', error);
            };

        } catch (error) {
            console.error('Failed to create WebSocket connection:', error);
        }
    }

}

// Initialize the app when the DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    new SystemViewApp();
});