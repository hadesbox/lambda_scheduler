import sys
import boto3
import boto
import datetime

from boto3.session import Session

curtime = datetime.datetime.now()
curday = datetime.datetime.today()

curday_text = "awake-weekday" if curday.weekday() <= 4 else "awake-weekend"

print "TIME", curtime, "CURRENT HOUR", curtime.hour,  "DAY OF WEEK", curday.weekday()

print "CHECKING EC2 INSTANCES"

session = Session(region_name='eu-west-1')

try:

    ec2 = session.resource('ec2')
   
    for inst in ec2.instances.filter(Filters = [{'Name': 'tag:'+curday_text, 'Values' : ['*']}]): 
        print "INSTANCE %s (%s) IS [%s]" % (inst.id, inst.tags, inst.state["Name"])
        current_schedules = None
        for tag in inst.tags:
            if tag["Key"] == curday_text :
                current_schedules = tag
        if current_schedules != None:
            if inst.state["Name"] == "stopped":
                if str(curtime.hour) in (x.strip() for x in current_schedules["Value"].split(" ")):
                    print "STARTING EC2 %s" % (inst.id)
                    inst.start()
            elif inst.state["Name"] == "running":
                if str(curtime.hour) not in (x.strip() for x in current_schedules["Value"].split(" ")):
                    print "STOPPING EC2 %s" % (inst.id)
                    inst.stop()
        else:
            print "NO SCHEDULES FOUND"
       
except Exception as e:
    print >> sys.stderr, "EC2 exception", e

print "CHECKING REDSHIFT CLUSTERS"

try:

    redshift = boto3.client('redshift')

    for snap in redshift.describe_cluster_snapshots()["Snapshots"]: 
        current_schedules = None
        for tag in snap["Tags"]:
            if tag["Key"] == curday_text :
                current_schedules = tag
                break
        if current_schedules != None:
            #we need to fetch if there is a cluster running from this snapshot
            try:
                status = redshift.describe_clusters()
                print status.Clusters[0]
                print "CLUSTER FROM SNAPSHOT %s (%s) FOUND IS [%s]" % (snap["SnapshotIdentifier"], current_schedules["Value"], status["DescribeClustersResponse"]["DescribeClustersResult"]["Clusters"][0]["ClusterStatus"])
                if status["DescribeClustersResponse"]["DescribeClustersResult"]["Clusters"][0]["ClusterStatus"] == "available" and str(curtime.hour) not in (x.strip() for x in current_schedules["Value"].split(" ")):
                    print "STOPPING REDSFHIT %s" % (snap["SnapshotIdentifier"])
                    redshift.delete_cluster(snap["SnapshotIdentifier"], skip_final_cluster_snapshot=True)
            except Exception as ex:
                print "CLUSTER FROM SNAPSHOT %s (%s) NOT FOUND" % (snap["SnapshotIdentifier"], current_schedules["Value"])
                if str(curtime.hour) in (x.strip() for x in current_schedules["Value"].split(" ")):
                    print "STARTING REDSHIFT %s" % (snap["SnapshotIdentifier"])
                    redshift.restore_from_cluster_snapshot(ClusterIdentifier=snap["SnapshotIdentifier"], SnapshotIdentifier=snap["SnapshotIdentifier"])
            except Exception as ex:
                print >> sys.stderr, "EC2 exception({0})".format(ex)
    
except Exception as e:
    print >> sys.stderr, "REDSHIFT exception({0})".format(e)

