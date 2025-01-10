from iceload import IceLoad

if __name__ == "__main__":
    # il1 = IceLoad("cmlc099", [], ["create", "insert"],
    #               "data/cmlc0993/config.yaml")
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
    il1 = IceLoad("cmlc01c5", [], ["dropcreate", "insert"],
                  "/home/alpine/iceload/data/cmlc01c5/config.yaml")
    il1.run_action()
    il1.finish()
