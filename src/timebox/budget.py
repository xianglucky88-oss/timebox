"""单一职责：对记录做单一职责变换：budget。（基线朴素实现，已接入 happy-path）"""
def process(record):
    rec = dict(record)
    rec.setdefault("_stage_budget", "ok")
    return rec
