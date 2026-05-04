FROM ollama/ollama:latest

COPY deploy/ollama/start-ollama.sh /usr/local/bin/start-ollama.sh
RUN chmod +x /usr/local/bin/start-ollama.sh

ENV OLLAMA_HOST=0.0.0.0:11434
ENV OLLAMA_MODELS=/var/data/ollama/models
ENV OLLAMA_MODEL=llama3.2:1b

EXPOSE 11434

ENTRYPOINT ["/usr/local/bin/start-ollama.sh"]
