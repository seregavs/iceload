from iceload import IceLoad

if __name__ == "__main__":
    # il1 = IceLoad("cmlc7o99", ["/home/alpine/iceload/data/cmlc7o99/t1/cmlc70993_20241220.parquet"],
    #               ["dropcreate1", "dropcreate2", "insert1", "merge12"],
    #               "/home/alpine/iceload/data/cmlc7o99/config.yaml")
    # il1 = IceLoad("plant", [], ["drop", "create", "insert", "recreate_attr"],
    #               "data/plant/config.yaml")              
    # il1 = IceLoad("plant", [], ["delete", "insert"],
    #               "/home/alpine/iceload/data/plant/config.yaml")
    # il1 = IceLoad("material", [], ["dropcreate", "insert"],
    #               "/home/alpine/iceload/data/material/config.yaml")
    # il1 = IceLoad("move_type", [], ["dropcreate", "insert", "checks"],
    #               "data/move_type/config.yaml")
    # il1 = IceLoad("cmlc07p32", [], ["dropcreate2", "insert2"],
    #               "data/cmlc07p327/config.yaml")
    # il1 = IceLoad("cmlc07p34", [], ["dropcreate2", "insert2"],
    #               "data/cmlc07p347/config.yaml")
    # il1 = IceLoad("cmlc099", [], ["dropcreate1", "dropcreate2", "insert1", "merge12"],
    #               "data/cmlc0993/config.yaml")
    # il1 = IceLoad("cmlc099", [], ["views"], "data/cmlc0993/config.yaml")
    # il1 = IceLoad("cmlc099", [], ["dropcreate2", "merge12"],
    #               "data/cmlc0993/config.yaml")
    # il1 = IceLoad("cmlc07p33", [], ["dropcreate1", "dropcreate2", "insert1","merge12"],
    #               "data/cmlc07p331/config.yaml")
    # il1 = IceLoad("cmlc01c1", [], ["dropcreate", "insert"],
    #               "/home/alpine/iceload/data/cmlc01c1/config.yaml")
    # il1 = IceLoad("cmlc01c2", [], ["dropcreate", "insert"],
    #               "/home/alpine/iceload/data/cmlc01c2/config.yaml")
    il1 = IceLoad("cmlc01c", ["/home/alpine/iceload/data/cmlc01c/t1/cmlc01c1_20250110.orc"], 
                  ["dropcreate1", "insert1"], "/home/alpine/iceload/data/cmlc01c/config.yaml")
    # il1 = IceLoad("cmlc01c", ["/home/alpine/iceload/data/cmlc01c/t2/cmlc01c2_20250110.orc"], 
    #               ["dropcreate2", "insert2"], "/home/alpine/iceload/data/cmlc01c/config.yaml")
    # il1 = IceLoad("cmlc01c", ["/home/alpine/iceload/data/cmlc01c/t5/cmlc01c5_20250110.orc"], 
    #               ["dropcreate5", "insert5"], "/home/alpine/iceload/data/cmlc01c/config.yaml")

    il1.run_action()
    il1.finish()
