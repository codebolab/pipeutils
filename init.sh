! grep -q "miniconda/bin" $HOME/.bashrc && echo 'export PATH="$HOME/miniconda/bin:$PATH"' >> $HOME/.bashrc
! grep -q "miniconda/bin/activate" $HOME/.bashrc && echo 'source $HOME/miniconda/bin/activate' >> $HOME/.bashrc
