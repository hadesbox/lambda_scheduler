import sys
import boto
import datetime

from boto import ec2
from boto import redshift

curtime = datetime.datetime.now()
curday = datetime.datetime.today()

curday_text = "awake-weekday" if curday.weekday() <= 4 else "awake-weekend"

print "TIME", curtime, "CURRENT HOUR", curtime.hour,  "DAY OF WEEK", curday.weekday()

print "CHECKING EC2 INSTANCES"

try:

    ec2_conn = boto.ec2.connect_to_region('eu-west-1')
    ec2_res = ec2_conn.get_all_instances(filters={"tag:"+curday_text : "*"})
    ec2_ins = [i for r in ec2_res for i in r.instances]
    for inst in ec2_ins:
        print "INSTANCE %s (%s) IS [%s]" % (inst.id, inst.tags[curday_text], inst.state)
        if inst.state == "stopped":
            if str(curtime.hour) in (x.strip() for x in inst.tags[curday_text].split(" ")):
                print "STARTING EC2 %s" % (inst.id)
                ec2_conn.start_instances(instance_ids=[inst.id])
        elif inst.state == "running":
            if str(curtime.hour) not in inst.tags[curday_text].split(" "):
                print "STOPPING EC2 %s" % (inst.id)
                ec2_conn.stop_instances(instance_ids=[inst.id])

except Exception as e:
    print >> sys.stderr, "EC2 exception({0})".format(e)

print "CHECKING REDSHIFT CLUSTERS"

try:

    red_conn = boto.redshift.connect_to_region('eu-west-1')
    red_snap =  red_conn.describe_cluster_snapshots()
    for snap in red_snap["DescribeClusterSnapshotsResponse"]["DescribeClusterSnapshotsResult"]["Snapshots"]:
        current_schedules = None
        for tag in snap["Tags"]:
            if tag["Key"] == curday_text :
                current_schedules = tag
                break
        if current_schedules != None:
            #we need to fetch if there is a cluster running from this snapshot
            try:
                status = red_conn.describe_clusters(cluster_identifier=snap["SnapshotIdentifier"])
                print "SNAPSHOT %s (%s) is [%s]" % (snap["SnapshotIdentifier"], current_schedules["Value"], status["DescribeClustersResponse"]["DescribeClustersResult"]["Clusters"][0]["ClusterStatus"])
                if status["DescribeClustersResponse"]["DescribeClustersResult"]["Clusters"][0]["ClusterStatus"] == "available" and str(curtime.hour) not in (x.strip() for x in current_schedules["Value"].split(" ")):
                    print "STOPPING REDSFHIT %s" % (snap["SnapshotIdentifier"])
                    red_conn.delete_cluster(snap["SnapshotIdentifier"], skip_final_cluster_snapshot=True)
            except boto.redshift.exceptions.ClusterNotFound as ex:
                print "SNAPSHOT %s (%s) is [stopped]" % (snap["SnapshotIdentifier"], current_schedules["Value"])
                if str(curtime.hour) in (x.strip() for x in current_schedules["Value"].split(" ")):
                    print "STARTING REDSHIFT %s" % (snap["SnapshotIdentifier"])
                    red_conn.restore_from_cluster_snapshot(cluster_identifier=snap["SnapshotIdentifier"], snapshot_identifier=snap["SnapshotIdentifier"])
            except Exception as ex:
                print >> sys.stderr, "EC2 exception({0})".format(ex)

except Exception as e:
    print >> sys.stderr, "REDSHIFT exception({0})".format(e)

