#!/usr/bin/env bash
set -x

tmux new-session -t 'wuklab-ext' -d
tmux split-window -v
tmux split-window -v
tmux send-keys -t 0 'ssh wuklab@wuklab-06.ucsd.edu' Enter
tmux send-keys -t 1 'ssh wuklab@wuklab-07.ucsd.edu' Enter
tmux send-keys -t 2 'ssh wuklab@wuklab-08.ucsd.edu' Enter
tmux select-layout even-vertical
# bind for this window until it dies
tmux bind-key e set-window-option synchronize-panes
# set option once
tmux set-option synchronize-panes on
tmux attach-session -t 'wuklab-ext'
