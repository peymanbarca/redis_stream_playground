Code for [my post](https://tirkarthi.github.io/programming/2018/08/21/redis-streams-python.html) on redis streams.

### Requirements

* Python 3.6+
* Redis 5.0 (RC 1) and above that has streams or use the unstable branch to compile Redis yourself.

### Installation

* Clone the repo
* Activate virtualenv and install requirements with `pip install -r requirements.txt`
* to instantiate redis node, run `docker-compose up -d`   
* To injest data `python producer.py`
* To start a consumer `python consumer.py <consumer_name>`

