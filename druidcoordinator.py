import argparse
import boto.ec2
import json
import logging
import os
import sys

#  Usage:
#    python druidcoordinator.py --cluster-tag-value [cluster_name]
#
class DruidCoordinatorDiscovery(object):
    def __init__(self, cluster_tag_key, cluster_tag_value, layer_tag_key, layer_tag_value, output_path):
        self.cluster_tag_key = cluster_tag_key
        self.cluster_tag_value = cluster_tag_value
        self.layer_tag_key = layer_tag_key
        self.layer_tag_value = layer_tag_value
        self.output_path = output_path

        self.ec2 = boto.ec2.connect_to_region(self._get_current_region())

    def _get_current_region(self):
        if self._is_running_on_ec2():
            return boto.utils.get_instance_identity()['document']['region']
        else:
            return 'us-east-1'

    def _is_running_on_ec2(self):
        os_version = os.getenv('os.version')
        return os_version is not None and 'amzn' in os_version

    def execute(self):
        tag_filter = {
            'tag:{}'.format(self.cluster_tag_key): self.cluster_tag_value,
            'tag:{}'.format(self.layer_tag_key): self.layer_tag_value
        }
        instances = self.ec2.get_only_instances(filters=tag_filter)
        for instance in instances:
            if instance.private_ip_address is not None:
                coordinator_ip = instance.private_ip_address
		print coordinator_ip
                break
        else:
            raise StandardError('Unable to find an instance with the required tags: {}'.format(tag_filter))

def main():
    parser = argparse.ArgumentParser(description='Discovery coordinator ip')
    parser.add_argument('--cluster-tag-key', help='Tag key to discover the druid cluster', default='cluster')
    parser.add_argument('--cluster-tag-value', help='Tag value to discover the druid cluster', required=True)
    parser.add_argument('--layer-tag-key', help='Tag key to discover the coordination layer', default='layer')
    parser.add_argument('--layer-tag-value', help='Tag value to discover the coordination layer', default='coordination')
    parser.add_argument('--output-path', help='Output directory for coordinator ip', default='./')
    args = parser.parse_args()

    discovery = DruidCoordinatorDiscovery(args.cluster_tag_key, args.cluster_tag_value, args.layer_tag_key, args.layer_tag_value, args.output_path)
    discovery.execute()

if __name__ == '__main__':
    main()
