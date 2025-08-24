class SystemViewApp {
    constructor() {
        this.ws = null;
        this.connect();
        this.initElements();
        this.loadInitialView();
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
                console.log("chunks", data.messages);
                this.messageTracks.chunks.innerHTML = data.messages.map(message => `<div class="message">${message.value.id}</div>`).join('');
            });

        fetch(`/api/topics/scenes/messages`)
            .then(response => response.json())
            .then(data => {
                console.log("scenes", data.messages);
                this.messageTracks.scenes.innerHTML = data.messages.map(message => `<div class="message">${message.value.scene_id}</div>`).join('');
            });
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