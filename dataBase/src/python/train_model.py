#!/usr/bin/env python3
"""机器学习 — 价格方向预测 (v3 精简版)"""
import pandas as pd, numpy as np, joblib, os

CSV_DIR = r"D:\桌面\dataBase_03\dataBase"
MODEL_OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "model_direction.joblib")

SYMBOLS = ["GOLD","OIL","SILVER","COPPER","GAS"]
FILES = {"GOLD":"gold_tick_data.csv","OIL":"oil_tick_data.csv","SILVER":"silver_tick_data.csv","COPPER":"copper_tick_data.csv","GAS":"gas_tick_data.csv"}

def featurize(df):
    p = df["price"]; v = df["volume"]
    df["sma5"]  = p.rolling(5).mean()
    df["sma10"] = p.rolling(10).mean()
    df["sma20"] = p.rolling(20).mean()
    df["sma_5_10"] = (df["sma5"]-df["sma10"])/(p+1e-10)*100
    df["sma_10_20"]= (df["sma10"]-df["sma20"])/(p+1e-10)*100
    delta = p.diff(); g = delta.clip(lower=0); l = -delta.clip(upper=0)
    rs = (g.rolling(14).mean())/(l.rolling(14).mean()+1e-10)
    df["rsi"] = 100-100/(1+rs)
    bb = p.rolling(20)
    df["bb_pos"] = (p-bb.mean())/(bb.std()*2+1e-10)
    df["mom5"] = p.pct_change(5)*100
    df["mom10"]= p.pct_change(10)*100
    vm = v.rolling(5).mean()
    df["vol_ratio"] = v/(vm+1e-10)
    df["vol_chg"] = v.pct_change(5)*100
    df["chg1"] = p.pct_change(1)*100
    df["lag1"] = p.shift(1); df["lag2"] = p.shift(2)
    df["spread_p"] = df["spread"]/(p+1e-10)*100
    df["hl_p"]  = (df["high"]-df["low"])/(p+1e-10)*100
    df["bam"]   = (df["bid"]+df["ask"])/2-p
    df["volat"] = df["volatility"]*10000
    df["target"]= (df["direction"].shift(-1)=="UP").astype(int)
    df = df.dropna()
    cols = ["lag1","lag2","chg1","sma_5_10","sma_10_20","rsi","mom5","mom10","bb_pos","vol_ratio","vol_chg","spread_p","hl_p","bam","volat"]
    return df[cols].values, df["target"].values

def main():
    print("="*50)
    print("  ML Training v3")
    print("="*50)
    Xa, ya = [], []
    for sym in SYMBOLS:
        f = os.path.join(CSV_DIR, FILES[sym])
        if os.path.exists(f):
            X,y = featurize(pd.read_csv(f))
            Xa.append(X); ya.append(y)
            print(f"  {sym}: {len(X):,} samples")
    X = np.vstack(Xa); y = np.concatenate(ya)
    print(f"  Total: {len(X):,} | UP: {y.mean():.1%}")

    from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier, VotingClassifier
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import accuracy_score

    Xt,Xv,yt,yv = train_test_split(X,y,test_size=0.2,random_state=42)

    model = VotingClassifier([
        ("gb", GradientBoostingClassifier(n_estimators=50,max_depth=5,random_state=42)),
        ("rf", RandomForestClassifier(n_estimators=50,max_depth=10,random_state=42,n_jobs=1))
    ], voting="soft")
    model.fit(Xt,yt)
    acc = accuracy_score(yv, model.predict(Xv))
    print(f"  Accuracy: {acc:.2%} | Baseline: {yv.mean():.1%}")

    joblib.dump({"model":model,"accuracy":acc,"features":15,"cols":["lag1","lag2","chg1","sma_5_10","sma_10_20","rsi","mom5","mom10","bb_pos","vol_ratio","vol_chg","spread_p","hl_p","bam","volat"]}, MODEL_OUT)
    print(f"  Saved: {MODEL_OUT} ({os.path.getsize(MODEL_OUT)/1024:.0f} KB)")

if __name__=="__main__": main()
