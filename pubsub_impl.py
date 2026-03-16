#!/usr/bin/env python3
"""Pub/Sub messaging with topics, filters, and dead letter queue."""
import sys, time

class Message:
    def __init__(self,topic,data,headers=None):
        self.topic=topic;self.data=data;self.headers=headers or {};self.ts=time.time();self.id=id(self)

class PubSub:
    def __init__(self):
        self.subscribers={};self.dlq=[];self.history=[]
    def subscribe(self,topic,callback,filter_fn=None):
        self.subscribers.setdefault(topic,[]).append({"cb":callback,"filter":filter_fn})
    def publish(self,topic,data,headers=None):
        msg=Message(topic,data,headers); self.history.append(msg); delivered=0
        for sub in self.subscribers.get(topic,[]):
            if sub["filter"] and not sub["filter"](msg): continue
            try: sub["cb"](msg); delivered+=1
            except Exception as e: self.dlq.append((msg,str(e)))
        return delivered
    def replay(self,topic,since=0):
        return [m for m in self.history if m.topic==topic and m.ts>=since]

if __name__ == "__main__":
    ps=PubSub(); log=[]
    ps.subscribe("orders",lambda m:log.append(f"handler1: {m.data}"))
    ps.subscribe("orders",lambda m:log.append(f"handler2: {m.data}"),
                filter_fn=lambda m:m.data.get("amount",0)>50)
    ps.subscribe("orders",lambda m:1/0)  # Will fail -> DLQ
    ps.publish("orders",{"item":"widget","amount":100})
    ps.publish("orders",{"item":"gadget","amount":25})
    print("Delivered:"); 
    for l in log: print(f"  {l}")
    print(f"Dead letters: {len(ps.dlq)}")
