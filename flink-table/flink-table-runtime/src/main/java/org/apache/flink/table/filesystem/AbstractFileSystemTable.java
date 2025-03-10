/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.filesystem;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.filesystem.FileSystemConnectorOptions.PARTITION_DEFAULT_NAME;
import static org.apache.flink.table.filesystem.FileSystemConnectorOptions.PATH;

/** Abstract File system table for providing some common methods. */
abstract class AbstractFileSystemTable {

    final DynamicTableFactory.Context context;
    final ObjectIdentifier tableIdentifier;
    final Configuration tableOptions;
    final ResolvedSchema schema;
    final Path path;
    final String defaultPartName;

    List<String> partitionKeys;

    AbstractFileSystemTable(DynamicTableFactory.Context context) {
        this.context = context;
        this.tableIdentifier = context.getObjectIdentifier();
        this.tableOptions = new Configuration();
        context.getCatalogTable().getOptions().forEach(tableOptions::setString);
        this.schema = context.getCatalogTable().getResolvedSchema();
        this.path = new Path(tableOptions.get(PATH));
        this.defaultPartName = tableOptions.get(PARTITION_DEFAULT_NAME);

        this.partitionKeys = context.getCatalogTable().getPartitionKeys();
    }

    ReadableConfig formatOptions(String identifier) {
        return new DelegatingConfiguration(tableOptions, identifier + ".");
    }

    DataType getPhysicalDataType() {
        return this.schema.toPhysicalRowDataType();
    }

    DataType getPhysicalDataTypeWithoutPartitionColumns() {
        return DataType.getFields(getPhysicalDataType()).stream()
                .filter(field -> !partitionKeys.contains(field.getName()))
                .collect(Collectors.collectingAndThen(Collectors.toList(), DataTypes::ROW));
    }
}
