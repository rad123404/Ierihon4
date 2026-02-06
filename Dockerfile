FROM python:3.12-slim

WORKDIR /app

# Копируем всё из репозитория
COPY . .

# Устанавливаем зависимости (добавь свои пакеты, если есть requirements.txt)
RUN pip install --no-cache-dir python-telegram-bot aiofiles

# Если у тебя есть requirements.txt → раскомментируй это и закомментируй строку выше
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1

CMD ["python", "IERIHON2.py"]
