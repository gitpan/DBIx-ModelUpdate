package DBIx::ModelUpdate::Oracle;

no warnings;

use Data::Dumper;

our @ISA = qw (DBIx::ModelUpdate);

################################################################################

sub unquote_table_name {
	my ($self, $name) = @_;
	$name =~ s{\W}{}g;
	return lc $name;
}

################################################################################

sub prepare {

	my ($self, $sql) = @_;
	
print STDERR "prepare (pid=$$): $sql\n";

	return $self -> {db} -> prepare ($sql);

}

################################################################################

sub get_keys {

	my ($self, $table_name) = @_;
	
	my $keys = {};
	
	my $st = $self -> prepare (<<EOS);
		SELECT 
			* 
		FROM 
			user_indexes
			, user_ind_columns 
		WHERE 
			user_ind_columns.index_name = user_indexes.index_name 
			AND user_indexes.table_name = ?
EOS
	
	$st -> execute (uc $table_name);
		
	while (my $r = $st -> fetchrow_hashref) {
		
		my $name = lc $r -> {INDEX_NAME};
		$name =~ s/^${table_name}_//;
		
		next if $name eq 'PRIMARY';
		
		my $column = lc $r -> {COLUMN_NAME};
		
		if (exists $keys -> {$name}) {
			$keys -> {$name} -> {columns} .= ',' . $column;
		}
		else {
			$keys -> {$name} = {columns => $column};
		}
	
	}
	
#print STDERR Dumper ($keys);
		
	return $keys;

}

################################################################################

sub get_tables {

	my ($self, $options) = @_;
	
	my $st = $self -> prepare ("SELECT table_name FROM user_tables");
	$st -> execute;
	my $tables = {};
	
	while (my $r = $st -> fetchrow_hashref) {
		my $name = lc ($r -> {TABLE_NAME});
		$name =~ s{\W}{}g;
		$tables -> {$name} = {
#			columns => $self -> get_columns ($name, $options), 
#			keys => $self -> get_keys ($name),
		}
	}	

	$st -> finish;

print STDERR "get_tables (pid=$$): $tables = " . Dumper ($tables);
	
	foreach my $name (keys %$tables) {
		$tables -> {$name} -> {columns} = $self -> get_columns ($name, $options);
		$tables -> {$name} -> {keys}    = $self -> get_keys ($name);
	}
	
	return $tables;

}

################################################################################

sub get_columns {

	my ($self, $table_name, $options) = @_;
	
	$options -> {default_columns} ||= {};
	
	my $uc_table_name = uc $table_name;
	
#	my $st = $st = $self -> prepare (<<EOS);
#		SELECT
#			user_ind_columns.COLUMN_NAME   
#		FROM 
#			user_constraints
#			, user_ind_columns  
#		WHERE
#			user_ind_columns.INDEX_NAME = user_constraints.CONSTRAINT_NAME  
#			and user_constraints.constraint_type = 'P' 
#			and user_constraints.table_name = ?
#EOS
#	
#	my $pk_column;
#	$st -> execute (uc $table_name);
#	while (($pk_column) = $st -> fetchrow_array) {
#print STDERR "get_columns (pid=$$): \$pk_column=$pk_column\n";
#	};
##	$st -> finish;
#	$pk_column = lc $pk_column;
		
	$pk_column = 'id';
		
	my $fields = {};
	
print STDERR "get_columns (pid=$$): \$table_name=$table_name\n";

#	my $st = $self -> prepare ("select * from user_tab_columns WHERE table_name = '$uc_table_name'");
	
	my $st = $self -> {db} -> column_info ('', $db -> {Username}, $uc_table_name, '');
	$st -> execute ();

#	$st -> execute (uc $table_name);
print STDERR "get_columns (pid=$$): execute completed\n";
		
	while (my $r = $st -> fetchrow_hashref) {
		
		$fields -> {lc $r -> {COLUMN_NAME}} = $r;
	
#		my $name = lc $r -> {COLUMN_NAME};
#		next if $options -> {default_columns} -> {$name};
#		
#		$r -> {TYPE_NAME} = $r -> {DATA_TYPE};
#		$r -> {COLUMN_SIZE} = $r -> {DATA_LENGTH};
#		$r -> {DECIMAL_DIGITS} = $r -> {DATA_PRECISION};
#		
#		if ($r -> {DATA_DEFAULT}) {
#			$r -> {COLUMN_DEF} = $r -> {DATA_DEFAULT};
#			$r -> {COLUMN_DEF} =~ s{^\'}{};
#			$r -> {COLUMN_DEF} =~ s{\'$}{};
#		}
#		
#		$r -> {_EXTRA} = $r -> {Extra} if $r -> {Extra};
#		$r -> {_PK} = 1 if $name eq $pk_column;
#		$r -> {NULLABLE} = $r -> {Null} eq 'YES' ? 1 : 0;
#		map {delete $r -> {$_}} grep {/[a-z]/} keys %$r;
#		$fields -> {$name} = $r;
	
	}
	
	return $fields;

}

################################################################################

sub get_canonic_type {

	my ($self, $type_name) = @_;
	
	$type_name = lc $type_name;
	
	return 'VARCHAR2' if $type_name eq 'varchar';
	return 'NUMBER'   if $type_name =~ /int$/;
	return 'NUMBER'   if $type_name eq 'decimal';
	return 'CLOB'     if $type_name eq 'text';
	return 'DATE'     if $type_name =~ /date|time/;
	
	return uc $type_name;

}    

################################################################################

sub gen_column_definition {

	my ($self, $name, $definition, $table_name) = @_;
	
	$definition -> {NULLABLE} = 1 unless defined $definition -> {NULLABLE};
	
	my $type = $self -> get_canonic_type ($definition -> {TYPE_NAME});

	my $sql = " $name $type";
		
	if ($definition -> {COLUMN_SIZE}) {	
		$sql .= ' (' . $definition -> {COLUMN_SIZE};		
		$sql .= ',' . $definition -> {DECIMAL_DIGITS} if $definition -> {DECIMAL_DIGITS};		
		$sql .= ')';	
	}
	
#	$sql .= ' ' . $definition -> {_EXTRA} if $definition -> {_EXTRA};

	if ($type eq 'CLOB') {
		$sql .= ' DEFAULT empty_clob()';
	} elsif (exists $definition -> {COLUMN_DEF}) {
		$sql .= ' DEFAULT ' . $self -> {db} -> quote ($definition -> {COLUMN_DEF});
	}

	$sql .= ' CONSTRAINT nn_' . $table_name . '_' . $name . ' NOT NULL' unless $definition -> {NULLABLE};
	$sql .= ' CONSTRAINT pk_' . $table_name . '_' . $name . ' PRIMARY KEY' if $definition -> {_PK};
	
	return $sql;
	
}

################################################################################

sub create_table {

	my ($self, $name, $definition) = @_;
	
	$self -> do ("CREATE TABLE $name (\n  " . (join "\n ,", map {$self -> gen_column_definition ($_, $definition -> {columns} -> {$_}, $name)} keys %{$definition -> {columns}}) . "\n)\n");

	$self -> do ("CREATE SEQUENCE ${name}_seq START WITH 1 INCREMENT BY 1");
	
	my $pk_column = (grep {$definition -> {columns} -> {$_} -> {_PK}} keys %{$definition -> {columns}}) [0];
	
	$self -> do (<<EOS);
		CREATE TRIGGER ${name}_id_trigger BEFORE INSERT ON ${name}
		FOR EACH ROW
		WHEN (new.$pk_column is null)
		BEGIN
			SELECT ${name}_seq.nextval INTO :new.$pk_column FROM DUAL;
		END;		
EOS

	$self -> do ("ALTER TRIGGER ${name}_id_trigger COMPILE");
	$self -> do ("ALTER TABLE ${name} ENABLE ALL TRIGGERS");

}

################################################################################

sub add_columns {

	my ($self, $name, $columns) = @_;
	
	my $sql = "ALTER TABLE $name ADD (\n  " . (join "\n ,", map {$self -> gen_column_definition ($_, $columns -> {$_}, $name)} keys %$columns) . "\n)\n";
			
	$self -> do ($sql);

}

################################################################################

sub get_column_def {

	my ($self, $column) = @_;
	
	return '' if $column -> {_PK};
	
	return $column -> {COLUMN_DEF} if defined $column -> {COLUMN_DEF};
	
	return 0 if lc $column -> {TYPE_NAME} =~ /bit|int|float|numeric|decimal|number/;
	
	return '';

}    

################################################################################

sub update_column {

	my ($self, $name, $c_name, $existing_column, $c_definition) = @_;
	
	my $existing_def = $self -> get_column_def ($existing_column);
	my $column_def = $self -> get_column_def ($c_definition);
	
	my $eq_types = ($self -> get_canonic_type ($existing_column -> {TYPE_NAME}) eq $self -> get_canonic_type ($c_definition -> {TYPE_NAME}));
	my $eq_sizes = ($existing_column -> {COLUMN_SIZE} >= $c_definition -> {COLUMN_SIZE});
	my $eq_defaults = ($existing_def eq $column_def);

#print STDERR '$existing_type = ', $self -> get_canonic_type ($existing_column -> {TYPE_NAME}), "\n";
#print STDERR '$c_type = ', $self -> get_canonic_type ($c_definition -> {TYPE_NAME}), "\n";
#print STDERR "\$eq_types = $eq_types\n";
#print STDERR "\$eq_sizes = $eq_sizes\n";
#print STDERR "\$eq_defaults = $eq_defaults\n";

	return if $eq_types && $eq_sizes && $eq_defaults;
	
	return if $self -> get_canonic_type ($existing_column -> {TYPE_NAME}) =~ /LOB/;
	
	$c_definition -> {_PK} = 0 if ($existing_column -> {_PK} == 1);
	delete $c_definition -> {NULLABLE} if (exists $existing_column -> {NULLABLE} && $existing_column -> {NULLABLE} == 0);

	my $sql = "ALTER TABLE $name MODIFY" . $self -> gen_column_definition ($c_name, $c_definition, $name);
	
	$self -> do ($sql);
	
}

################################################################################

sub insert_or_update {

	my ($self, $name, $data, $table) = @_;
	
	my $pk_column = 'id';
	
	my $st = $self -> prepare ("SELECT * FROM $name WHERE $pk_column = ?");
	$st -> execute ($data -> {$pk_column});
	my $existing_data = $st -> fetchrow_hashref;
	$st -> finish;
	
	if ($existing_data -> {uc $pk_column}) {
	
		my @terms = ();
		
		while (my ($key, $value) = each %$data) {
			next if $key eq $pk_column;
			next if $value eq $existing_data -> {uc $key};
			push @terms, "$key = " . $self -> {db} -> quote ($value);
		}
		
		if (@terms) {
			$self -> do ("UPDATE $name SET " . (join ', ', @terms) . " WHERE $pk_column = " . $data -> {$pk_column});
		}
	
	}
	else {
		my @names = keys %$data;
		$self -> do ("INSERT INTO $name (" . (join ', ', @names) . ") VALUES (" . (join ', ', map {$self -> {db} -> quote ($data -> {$_})} @names) . ')');
	}

}

################################################################################

sub drop_index {
	
	my ($self, $table_name, $index_name) = @_;
	
	$self -> {db} -> do ("DROP INDEX ${table_name}_${index_name}");
	
}

################################################################################

sub create_index {
	
	my ($self, $table_name, $index_name, $index_def) = @_;
		
	$self -> {db} -> do ("CREATE INDEX ${table_name}_${index_name} ON $table_name ($index_def)");
	
}

1;