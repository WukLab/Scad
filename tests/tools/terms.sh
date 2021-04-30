#!/usr/bin/env bash

tmux new-session -t 'wuklab' -d
tmux split-window -v
tmux split-window -v
tmux split-window -v
tmux split-window -v
tmux send-keys -t 0 'ssh wuklab@wuklab-02.ucsd.edu' Enter
tmux send-keys -t 1 'ssh wuklab@wuklab-03.ucsd.edu' Enter
tmux send-keys -t 2 'ssh wuklab@wuklab-04.ucsd.edu' Enter
tmux send-keys -t 3 'ssh wuklab@wuklab-05.ucsd.edu' Enter
tmux select-layout even-vertical
# bind for this window until it dies
tmux bind-key e set-window-option synchronize-panes
# set option once
tmux set-option synchronize-panes on
tmux -2 attach-session -t 'wuklab'
