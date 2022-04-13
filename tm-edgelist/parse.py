#!/usr/bin/env python


##########################################################################################
# Some of the code is adapted from:
# https://github.com/crimson-unicorn/parsers/tree/master/streamspot
##########################################################################################


import sys
import math
import tqdm
import argparse


def read_single_graph(file_name):
    """Read a single graph with ID @graph_id from the file @file_name and return the list of its edges."""

    graph = list()                                              # list of parsed graph edges
    # list of the node id that we have seen already
    node_id_seen = list()
    # logical timestamps of edges
    cnt = 1

    desc = "\x1b[6;30;42m[STATUS]\x1b[0m Parsing darpa-e5 data from {}".format(file_name)

    pb = tqdm.tqdm(desc=desc, mininterval=1.0, unit=" edges")
    with open(file_name, "r") as f:
        for line in f:
            edge = line.strip().split("\t")

            pb.update()                                     # for progress tracking
            # check if we have seen the source node before
            if edge[0] in node_id_seen:
                # seen node is given 0 in the edge entry
                edge.append("0")
            else:
                # unseen node is given 1 in the edge entry
                edge.append("1")
                # the unseen node is now seen
                node_id_seen.append(edge[0])
            # check if we have seen the destination node before
            if edge[2] in node_id_seen:
                edge.append("0")
            else:
                edge.append("1")
                node_id_seen.append(edge[2])


            attributes = edge[2].strip().split(":")
            source_node_type = attributes[0]
            destination_node_type = attributes[1]
            edge_type = attributes[2]
            timestamp = attributes[3]
            
            edge[2] = source_node_type
            edge.append(destination_node_type)
            edge.append(edge_type)
            edge.append(timestamp)

            # # give the edge a logical timestamp
            # edge.append(cnt)
            # cnt = cnt + 1

            graph.append(edge)

    f.close()
    pb.close()
    return graph


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-g', '--graph', help='the id of the graph to be parsed', required=True)
    # %% is not a typo: https://thomas-cokelaer.info/blog/2014/03/python-argparse-issues-with-the-help-argument-typeerror-o-format-a-number-is-required-not-dict/
    parser.add_argument('-s', '--size', help='the size of the base graph in absolute value (default is 10%% of the entire graph)', type=int)
    parser.add_argument('-i', '--input', help='input StreamSpot data file path', required=True)
    parser.add_argument('-b', '--base', help='output file path of the base graph', required=True)
    parser.add_argument('-S', '--stream', help='output file path of the stream graph', required=True)
    args = parser.parse_args()

    graph = read_single_graph(args.input)
    if not args.size:
        base_graph_size = int(math.ceil(len(graph) * 0.1))
    else:
        base_graph_size = args.size
    stream_graph_size = len(graph) - base_graph_size

    base_file = open(args.base, "w")
    stream_file = open(args.stream, "w")

    '''
	    edge format: [source_node_id, destination_node_id, source_node_type, source_node_seen, destination_node_seen, destination_node_type, edge_type, timestamp]
	'''

    cnt = 0
    for edge in graph:
        if cnt < base_graph_size:
            cnt = cnt + 1
            base_file.write("{} {} {}:{}:{}:{}\n".format(edge[0], edge[1], edge[2], edge[5], edge[6], edge[7]))
        else:
            stream_file.write("{} {} {}:{}:{}:{}:{}:{}\n".format(edge[0], edge[1], edge[2], edge[5], edge[6], edge[3], edge[4], edge[7]))

    print("\x1b[6;30;42m[SUCCESS]\x1b[0m Graph {} is processed.".format(args.graph))
    print("\x1b[6;30;42m[SUCCESS]\x1b[0m Base graph of size {} is located at {}".format(base_graph_size, args.base))
    print("\x1b[6;30;42m[SUCCESS]\x1b[0m Stream graph of size {} is located at {}".format(stream_graph_size, args.stream))

    base_file.close()
    stream_file.close()
