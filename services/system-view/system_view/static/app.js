class SystemViewApp {
    constructor() {
        this.ws = null;
        this.connect();
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
                console.log('WebSocket message received:', event.data);
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