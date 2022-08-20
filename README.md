# Cryptopy

Cryptopy is our final project for NYU DS-GA.3001 Advanced Python programming.



[![UI](/figures/UI.png)](https://www.youtube.com/watch?v=UmX4kyB2wfghttps://www.youtube.com/watch?v=MSD3RArbnnQ)

[Demo Video](https://youtu.be/MSD3RArbnnQ)

## How to run main program

- Run the following to install required packages:
` pip3 install -r requirements.txt`

- Run `run.py` to start the program:
`python3 run.py`

- Click **System** - **Connect** and fill in the settings:
	- "API Key": "29855ca7-e615-4149-a4f6-1f2f6fa85603"
	- "Secret Key": "A3DBCD1CA9ED6410C25417D3DBA138DA"
	- "Passphrase": "NYUnyu1"	
	- "Server": "TEST"

## How to setup a strategy, and run it
- Click **Tools** - **PortfolioStrategy**
- Add **PairStrategy**
  - strategy_name is arbitrary. 
  - vt_symbols: BTC-USDT.OKEX,BTC-USDT-SWAP.OKEX
- Click **Init** - **Start**


## How to download historical minute data
Run the following to download 
`python download_data/record_history_multithread.py`

## How to record tick data
- Click **Tools** - **DataRecorder**
- Input **BTC-USDT.OKEX** to Local codes
- Click **Add** next to **Tick Recorder**

## How to train a new model
- Run **Model.ipynb** in **Model** folder
