# -*- coding: utf-8 -*-
"""
@author: CYRUS DSOUZA
"""

#--------------------------------------#

""" Predefined Modules """
import pyorient
#--------------------------------------#

""" Modules """
from util import errorcheck
import globs as gb
#--------------------------------------#

#client = pyorient.OrientDB(gb.ODB_IP, gb.ODB_PORT)
#client.connect(gb.ODB_NAME, gb.ODB_PWD)

#--------------------------------------#

class OrientDB(object):

    def __init__(self, database, fresh_ingest=False,
                 delete_mode=False,search_mode = False):

        database_valid = True
        
        self.client = pyorient.OrientDB(gb.ODB_IP, gb.ODB_PORT)
        self.client.connect(gb.ODB_NAME, gb.ODB_PWD)

        if delete_mode:
            if not self.client.db_exists(database):
                print("\nDB '{}' does not exist ..........").format(database)
                database_valid = False

        elif not self.client.db_exists(database):
            if search_mode :
                print("Database {} does not exist".format(database))
                database_valid = False
            
            else:
                print("Creating {} ..........").format(database)
                self.client.db_create(database, pyorient.DB_TYPE_GRAPH,
                             pyorient.STORAGE_TYPE_PLOCAL)

        elif self.client.db_exists(database) and fresh_ingest:
            print("Dropping {} ..........").format(database)
            # can be removed on demand. Check for Fresh ingest = False
            self.client.db_drop(database)
            print("Creating {} ..........").format(database)
            self.client.db_create(database, pyorient.DB_TYPE_GRAPH,
                             pyorient.STORAGE_TYPE_PLOCAL)
            self.client.db_open(database, gb.ODB_NAME, gb.ODB_PWD)

        if database_valid:
            try:
                self.client.db_open(database, gb.ODB_NAME, gb.ODB_PWD)
            except pyorient.exceptions.PyOrientDatabaseException as e:
                print("Database error with errors - {}".format(e))

        self.database = database
        self.database_valid = database_valid

    #--------------------------------------#

    def _close_db(self):
        print("Closing DB connection for database {}".format(self.database))
        self.client.db_close(self.database)

    def _drop_db(self,database_name):
        """ Database drop is encountering some issue with
            an integrated system hence making it separate.

            #TODO Issue : Bad file Descriptor Error 9 Socket Error
            #Definitely not the way to create a new connection and delete it but
            # I need to find another work around.

            Hence for now adopting the following fix."""

        if not self.client.db_exists(database_name):
            print("\nDB '{}' does not exist ..........").format(database_name)
        else:
            print("Deleting DB --- > {}".format(database_name))
            del_self_client = pyorient.OrientDB(gb.ODB_IP, gb.ODB_PORT)
            del_self_client.connect(gb.ODB_NAME, gb.ODB_PWD)
            del_self_client.db_drop(database_name)


    """ ORIENTDB EXECUTORS """

    @errorcheck
    def check_edge_exists_com(self, query):
        """ Check if the edge exists """

        res = self.client.command(query)

        return True if res else False

    #--------------------------------------#

    @errorcheck
    def get_rids(self, query):
        """ Processes the orientdb query and returns a list of ids for the retrieved documents """

        ids = [str(id_objs.rid) for id_objs in self.client.command(query)]

        return ids

    #--------------------------------------#
    @errorcheck
    def check_synonym_exists(self, node_class, ent, ent1):
        record = self.client.command("select name from %s where  ('%s' in synonym and name='%s')" % (
            node_class, str(ent), str(ent1)))

        return True if record else False

    @errorcheck
    def check_synonym_class(self, node_class, ent, ent1):
        record = self.client.command("select @class from %s where  ('%s' in synonym and @class='%s')" % (
            node_class, str(ent), str(ent1)))

        return True if record else False

    @errorcheck
    def check_node_exists(self, node_class, ent):

        record = self.client.command(
            "select name from %s where name = '%s'" % (node_class, str(ent)))

        return True if record else False



    @errorcheck
    def check_class_exists(self, query, classname):
        """ checks if a class exists in OrientDb database """

        dbClasses = [m.oRecordData.values()[0] for m in self.client.command(query)]

        return True if classname in dbClasses else False

    #--------------------------------------#

    @errorcheck
    def create_class(self, classname, extends='V'):
        """ Create a class in OrientDB , inherited or not """

        if not self.check_class_exists(self.get_classes(classname), classname):
            self.client.command("Create class %s extends %s" % (classname, extends))
            return("Class {} created".format(classname))

        else:
            return("Class {} already exists in {}".format(classname, extends))

    #--------------------------------------#

    """ ORIENTDB QUERYMAKERS """

    @errorcheck
    def find_similar_nodes_query(self, node_class, ent):

        query = "select name from V where name like '%s'" % (str(ent))

        return query

    #--------------------------------------#

    @errorcheck
    def classic_sql(self, select, frm, data=False, agg=False, where=False):

        query = "select {}({}) from {} where {} == '{}'" \
            .format(agg, select, frm, where, data)
        return query

    #--------------------------------------#

    @errorcheck
    def check_node_exists_table(self, cls, ent):

        query = "select set(name) from {} where name = '{}'" \
            .format(cls, ent)
        return query

    #--------------------------------------#

    @errorcheck
    def delete_edge(self,classname, condition):
        """ Condition is a tuple ---> (argument, value)
            eg = (g_id, 1238862)"""

        """ Delete edges from a particular node """

        query = """

                    delete edge from
                                (select * from
                                         {} where
                                                 '{}' = '{}' )


                                """.format(classname, condition[0], condition[1])

        return query

    @errorcheck
    def get_classes(self, classname):
        """ Get all classes from OrientDB """

        query = " SELECT name from (select expand(classes) from metadata:schema) where '{}' in name ".format(
            classname)

        return query

    #--------------------------------------#

    @errorcheck
    def get_all_edges(self):
        """Get all the edges fron the database """

        query = """SELECT set(name) FROM (SELECT expand(classes) FROM metadata:schema)
                        where superClasses == 'E'"""

        return query

    #--------------------------------------#

    @errorcheck
    def get_all_vertices(self):
        """Get all the vertices fron the database """

        query = """SELECT set(name) FROM (SELECT expand(classes) FROM metadata:schema)
                        where superClasses == 'V'"""

        return query

    #--------------------------------------#

    @errorcheck
    def get_all_entities(self):
        """Get all the entities fron the database """

        query = " SELECT set(name) FROM V "

        return query

    #--------------------------------------#

    @errorcheck
    def insert_node(self, node_class, input_json={"name": '', "content": ''}):

        #if not self.check_node_exists(node_class, input_json.get('name','')):
        query = """ INSERT INTO %s CONTENT %s """ % (node_class, input_json)

        return query

    @errorcheck
    def create_edge(self, node_class, v1, v2, attributes={}, node_class_dest=False, edge_class=False, node1_attr=False, node2_attr=False):
        """
        node_class : Class you want to use to insert the edge in
        v1 : node 1
        v2 : node 2
        attributes : {} #currently only has by default, 'relationship, weight and metadata'
        edge_class : edge class to be used
        node1_Attr and node2_Attr : Attributes to create a node incase does not exist.

        """

        #--------------------------------------#

        if not self.check_node_exists(node_class, v1):
            if not node1_attr:
                self.client.command(self.insert_node(node_class, {"name": v1}))
            else:
                self.client.command(self.insert_node(node_class, node1_attr))

        if node_class_dest:  # check if theres a destination node class mentioned

            if not self.check_node_exists(node_class_dest, v2):
                if not node2_attr:
                    self.client.command(self.insert_node(
                        node_class_dest, {"name": v2}))
                else:
                    self.client.command(self.insert_node(
                        node_class_dest, node2_attr))

        else:  # use the common class between vertices.
            if not self.check_node_exists(node_class, v2):
                if not node2_attr:
                    self.client.command(self.insert_node(node_class, {"name": v2}))
                else:
                    self.client.command(self.insert_node(node_class, node2_attr))

        #--------------------------------------#

        """ Edge parameters and configuration """

        if edge_class and node_class_dest:
            #            print ("Source and destination classes present")

            # TODO check edge class validity

            query ="""create edge %s from (select from %s where name = "%s") to (select from %s
                  where name = "%s") content {relationship : '%s', tagged_doc : '%s',doc_id:'%s', weight : %s, metadata : %s}""" \
                  % (str(edge_class),str(node_class),str(v1),str(node_class_dest),str(v2),attributes.get('relationship',''),attributes.get('tagged_doc',""),attributes.get('doc_id',""), attributes.get('weight',str(0)),
                     attributes.get('metadata',{}))
        elif edge_class:
            #            print ("Edge Class Present")

            # TODO check edge class validity

            query ="""create edge %s from (select from %s where name = "%s") to (select from %s
                  where name = "%s") content {relationship : '%s', tagged_doc : '%s',doc_id:'%s', weight : %s, metadata : %s}""" \
                  % (str(edge_class),str(node_class),str(v1),str(node_class),str(v2),attributes.get('relationship',''),attributes.get('tagged_doc',""),attributes.get('doc_id',""), attributes.get('weight',str(0)),
                     attributes.get('metadata',{}))

        else:
#            print('Regular EDGE query')
            query ="""create edge from (select from %s where name = "%s") to (select from %s
                  where name = "%s") content {relationship : '%s', tagged_doc: '%s',doc_id:'%s', weight : %s, metadata : %s}""" \
                  % (str(node_class),str(v1),str(node_class),str(v2),attributes.get('relationship',''),attributes.get('tagged_doc',""),attributes.get('doc_id',""),  attributes.get('weight',str(0)),
                     attributes.get('metadata',{}))


        return query

    #--------------------------------------#

    @errorcheck
    def update_node(self, node_class, update_node, prop, merge=True):
        """
        Update a node of the class

        update_node : node to update
        prop : property json , {property: value}
        """

        #--------------------------------------#

        if not self.check_node_exists(node_class, update_node):
            self.client.command(self.insert_node(node_class, input_json=prop))

        if merge:
            query = "UPDATE %s MERGE %s where name = '%s' " % (
                node_class, prop, update_node)

        if not merge:

            query = "UPDATE %s CONTENT %s where name = '%s' " % (
                node_class, prop, update_node)

        return query

    #--------------------------------------#

    @errorcheck
    def set_schema(self, node_class, prop_name, datatype_standard, datatype_container=False):
        """

        Set the schema for the class properties.

        node_class : class/cluster of the node
        prop_name : property name
        datatype_standard : standard datatypes : integer, boolean, string, date, byte etc...
        datatype_container : container datatypes : embeddedlist, embeddedmap, linkedlist, linkset, linkmap etc..
        """

        #--------------------------------------#

        if datatype_container:
            query = "CREATE PROPERTY {}.{} {} {}".format(
                node_class, prop_name, datatype_container, datatype_standard)

        else:
            query = "CREATE PROPERTY {} {}.{}".format(
                node_class, prop_name, datatype_standard)

        return query

    #--------------------------------------#

    @errorcheck
    def get_rid(self, node_class, node):

        query = " SELECT @rid from %s where name = '%s' " % (node_class, node)

        return query

    #--------------------------------------#

    @errorcheck
    def check_edge_exists(self, n1_rid, n2_rid, edge_name, get_rid = False):
        """one direction src to dest """

        if get_rid : 
            #here n(i)_rid will be the node entity if get_rid is True
            n1_rid = self.client.command(self.get_rid(edge_name, n1_rid))
            n2_rid = self.client.command(self.get_rid(edge_name, n2_rid))

        query = " SELECT FROM {} WHERE OUT('{}').@rid CONTAINS {}".format(n1_rid,
                                                                          edge_name,
                                                                          n2_rid
                                                                          )

        return query

    #--------------------------------------#

    @errorcheck
    def check_edgeclass_exists(self, edge_name):

        query = """SELECT * FROM (SELECT expand(classes) FROM metadata:schema)
                        where superClasses == 'E' and name = '{}'""".format(
            edge_name

        )

        return query

    #--------------------------------------#

    @errorcheck
    def check_vertexclass_exists(self, class_name):
        """one direction src to dest """

        query = """SELECT * FROM (SELECT expand(classes) FROM metadata:schema)
                        where superClasses == 'V' and name = '{}'""".format(
            class_name

        )

        return query

    #--------------------------------------#
#    @errorcheck
    def get_table_from_relation(self, edge_name):
        """one direction src to dest """

        query = """
                    select set(@class) from

                    (select expand(out('{}')) from V ) """.format(edge_name)

        return query

    #--------------------------------------#
    @errorcheck
    def check_similar_edgeclasses(self, edge_name):
        """one direction src to dest """

        query = " SELECT set(@class) FROM E WHERE @class like '%{}%'".format(
            edge_name

        )

        return query

    #--------------------------------------#
    @errorcheck
    def check_similar_vertexclasses(self, vertex_name):
        """one direction src to dest """

        query = " SELECT set(@class) FROM V WHERE @class like '%{}%'".format(
            vertex_name

        )

        return query

    #--------------------------------------#

    @errorcheck
    def get_record(self, node_class, node):

        query = " SELECT * from %s where name = '%s' " % (node_class, node)

        return query

    #--------------------------------------#
