package DBIx::ModelUpdate::mysql;

no warnings;

use Data::Dumper;

our @ISA = qw (DBIx::ModelUpdate);

################################################################################

sub get_columns {

	my ($self, $table_name, $options) = @_;
	
	$options -> {default_columns} ||= {};
	
	my $fields = {};
	
	my $st = $self -> {db} -> prepare ("SHOW COLUMNS FROM $table_name");
	
	$st -> execute ();
		
	while (my $r = $st -> fetchrow_hashref) {
	
		my $name = $r -> {Field};
		next if $options -> {default_columns} -> {$name};
		
		$r -> {Type} =~ /^\w+/;
		$r -> {TYPE_NAME} = $&;
		$r -> {Type} =~ /\d+/;
		$r -> {COLUMN_SIZE} = $&;
		$r -> {COLUMN_DEF} = $r -> {Default} if $r -> {Default};
		$r -> {_EXTRA} = $r -> {Extra} if $r -> {Extra};
		$r -> {_PK} = 1 if $r -> {Key} eq PRI;
		$r -> {NULLABLE} = $r -> {Null} eq YES ? 1 : 0;
		map {delete $r -> {$_}} grep {/[a-z]/} keys %$r;
		$fields -> {$name} = $r;
	
	}
	
	return $fields;

}

################################################################################

sub gen_column_definition {

	my ($self, $name, $definition) = @_;
	
	$definition -> {NULLABLE} = 1 unless defined $definition -> {NULLABLE};

	my $sql = " $name $$definition{TYPE_NAME}";
	$sql .= ' (' . $definition -> {COLUMN_SIZE} . ')' if $definition -> {COLUMN_SIZE};
	$sql .= ' ' . $definition -> {_EXTRA} if $definition -> {_EXTRA};
	$sql .= ' NOT NULL' unless $definition -> {NULLABLE};
	$sql .= ' PRIMARY KEY' if $definition -> {_PK};
	$sql .= ' DEFAULT ' . $self -> {db} -> quote ($definition -> {COLUMN_DEF}) if $definition -> {COLUMN_DEF};
	
	return $sql;
	
}

################################################################################

sub create_table {

	my ($self, $name, $definition) = @_;
	
	my $sql = "CREATE TABLE $name (\n  " . (join "\n ,", map {$self -> gen_column_definition ($_, $definition -> {columns} -> {$_})} keys %{$definition -> {columns}}) . "\n)\n";
			
	$self -> {db} -> do ($sql);

}

################################################################################

sub add_columns {

	my ($self, $name, $columns) = @_;
	
	my $sql = "ALTER TABLE $name ADD (\n  " . (join "\n ,", map {$self -> gen_column_definition ($_, $columns -> {$_})} keys %$columns) . "\n)\n";
			
	$self -> {db} -> do ($sql);

}

################################################################################

sub update_column {

	my ($self, $name, $c_name, $existing_column, $c_definition) = @_;
	
	my $existing_def = $existing_column -> {COLUMN_DEF};
	$existing_def = '' unless defined $existing_def;
	
	my $column_def = $c_definition -> {COLUMN_DEF};
	$column_def = '' unless defined $column_def;
	
	return if 
		$existing_column -> {TYPE_NAME} eq $c_definition -> {TYPE_NAME} 
		and $existing_column -> {COLUMN_SIZE} >= $c_definition -> {COLUMN_SIZE}
		and $existing_def eq $column_def
	;
	
	my $sql = "ALTER TABLE $name CHANGE $c_name " . $self -> gen_column_definition ($c_name, $c_definition);
	
	$self -> {db} -> do ($sql);
	
}

################################################################################

sub insert_or_update {

	my ($self, $name, $data) = @_;

	my @names = keys %$data;

	my $sql = "REPLACE INTO $name (" . (join ', ', @names) . ") VALUES (" . (join ', ', map {$self -> {db} -> quote ($data -> {$_})} @names) . ')';

	$self -> {db} -> do ($sql);

}

1;