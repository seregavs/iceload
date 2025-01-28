from iceload import IceLoad

if __name__ == "__main__":
    ilList = []

    # il1 = IceLoad("cmlc7o99", ["/home/alpine/iceload/data/cmlc7o99/t1/cmlc70993_20241220.parquet"],
    #               ["dropcreate1", "dropcreate2", "insert1", "merge12"],
    #               "/home/alpine/iceload/data/cmlc7o99/config.yaml")
    # il1 = IceLoad("plant", [], ["drop", "create", "insert", "recreate_attr"],
    #               "data/plant/config.yaml")              
    # il1 = IceLoad("plant", [], ["delete", "insert"],
    #               "/home/alpine/iceload/data/plant/config.yaml")
    # il1 = IceLoad("plant", [], ["delete", "insert"],
    #               "s3a://stg-bi-1/0datasource/bwp/plant/config.yaml")
    # il1 = IceLoad("material", [], ["dropcreate", "insert"],
    #               "/home/alpine/iceload/data/material/config.yaml")
    # il1 = IceLoad("move_type", [], ["dropcreate", "insert", "checks"],
    #               "data/move_type/config.yaml")
    # il1 = IceLoad("cmlc07p32", [], ["dropcreate2", "insert2"],
    #               "data/cmlc07p327/config.yaml")
    # il1 = IceLoad("cmlc07p34", [], ["dropcreate2", "insert2"],
    #               "data/cmlc07p347/config.yaml")
    # il1 = IceLoad("cmlc7o99", [], ["dropcreate1", "dropcreate2", "insert1", "merge12"],
    #               "data/cmlc7o99/config.yaml")
    # il1 = IceLoad("cmlc7o99", [], ["dropcreate1", "dropcreate2", "insert1", "merge12"],
    #               "s3a://stg-bi-1/0datasource/bwp/cmlc7o99/config.yaml")
    # il1 = IceLoad("cmlc7o99", [], ["views"], "data/cmlc7o99/config.yaml")
    # il1 = IceLoad("cmlc7o99", [], ["dropcreate2", "merge12"],
    #               "data/cmlc7o99/config.yaml")
    # il1 = IceLoad("cmlc07p33", [], ["dropcreate1", "dropcreate2", "insert1", "merge12"],
    #               "/home/alpine/iceload/data/cmlc07p33/config.yaml")
    # ilList.append(IceLoad("cmlc01c", ["/home/alpine/iceload/data/cmlc01c/t1/cmlc01c1_20250110.orc"], 
    #               ["dropcreate1", "insert1"], "/home/alpine/iceload/data/cmlc01c/config.yaml"))
    # ilList.append(IceLoad("cmlc01c", ["/home/alpine/iceload/data/cmlc01c/t2/cmlc01c2_20250110.orc"], 
    #               ["dropcreate2", "insert2"], "/home/alpine/iceload/data/cmlc01c/config.yaml"))
    # ilList.append(IceLoad("cmlc01c", ["/home/alpine/iceload/data/cmlc01c/t5/cmlc01c5_20250110.orc"], 
    #               ["dropcreate5", "insert5"], "/home/alpine/iceload/data/cmlc01c/config.yaml"))

    # ilList.append(IceLoad("plant", [], ["delete", "insert"],
    #               "s3a://stg-bi-1/0datasource/bwp/plant/config.yaml"))
    ilList.append(IceLoad("cmlc07p33", [], ["insert2"],
                #   "/home/alpine/iceload/data/cmlc07p33/config.yaml",
                  "s3a://stg-bi-1/0datasource/bwp/cmlc07p33/config.yaml"))
    # ilList.append(IceLoad("cmlc01c", [],
    #               ["merge12"], "/home/alpine/iceload/data/cmlc01c/config.yaml"))
    for il in ilList:
        # il.srcbucket = 'stg-bi-1'
        # lst = il.get_s3('2')
        # print(lst)
        il.run_action()
        il.finish()

