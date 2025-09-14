#!/bin/bash
while true
do
  echo "Bot başlatılıyor..."
  python3 poster75_bot.py
  echo "Bot çöktü! 5 saniye içinde yeniden başlatılıyor..."
  sleep 5
done
