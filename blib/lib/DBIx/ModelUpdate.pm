package DBIx::ModelUpdate;

use 5.006;

require Exporter;

our $VERSION = '0.5';

use Data::Dumper;

no strict;
no warnings;

################################################################################

sub is_implemented {

	my ($driver_name) = @_;
	
	return $driver_name eq 'mysql' || $driver_name eq 'Oracle';

}

################################################################################

sub do {

	my ($self, $sql) = @_;
	
	print STDERR $sql, "\n" if $self -> {dump_to_stderr};
	
	$self -> {db} -> do ($sql);

}

################################################################################

sub new {

	my ($package_name, $db, @options) = @_;
	
	my $driver_name = $db -> {Driver} -> {Name};
	
	is_implemented ($driver_name) or die ("DBIx::ModelUpdate error: $driver_name driver is not supported");
	
	$package_name .= "::$driver_name";
	
	eval "require $package_name";
	
	die $@ if $@;
	
	bless ({db => $db, @options}, $package_name);

}

################################################################################
#
#sub unquote_table_name {
#	my ($name) = @_;
#	$name =~ s{\W}{}g;
#	return lc $name;
#}

################################################################################

sub get_tables {

	my ($self, $options) = @_;
	
	return { 
		map { 
			$_ => { columns => $self -> get_columns ($_, $options), keys => $self -> get_keys ($_) } 
		} 
		map {
			$self -> unquote_table_name ($_)
		} 
		($self -> {db} -> tables)
	};

}

################################################################################

sub assert {

	my ($self, %params) = @_;
	
	my $needed_tables = $params {tables};
	
	my $existing_tables = $self -> get_tables;	
	
	while (my ($name, $definition) = each %$needed_tables) {
	
		while (my ($dc_name, $dc_definition) = each %{$params {default_columns}}) {
			
			$definition -> {columns} -> {$dc_name} ||= $dc_definition;
			
		};

		if ($existing_tables -> {$name}) {
		
			my $existing_columns = $existing_tables -> {$name} -> {columns};
					
			my $new_columns = {};
				
			while (my ($c_name, $c_definition) = each %{$definition -> {columns}}) {
			
				if ($existing_columns -> {$c_name}) {
				
					my $existing_column = $existing_columns -> {$c_name};
										
					$self -> update_column ($name, $c_name, $existing_column, $c_definition);
								
				}
				else {
				
					$new_columns -> {$c_name} = $c_definition;
				
				}

			};
				
			$self -> add_columns ($name, $new_columns) if keys %$new_columns;

			while (my ($k_name, $k_definition) = each %{$definition -> {keys}}) {
			
				$k_definition -> {columns} =~ s{\s+}{}g;			
			
				if (
					$existing_tables -> {$name} 
					&& $existing_tables -> {$name} -> {keys} -> {$k_name}
				) {
				
					if ($existing_tables -> {$name} -> {keys} -> {$k_name} -> {columns} ne $k_definition) {
						$self -> drop_index ($name, $k_name);
					}
					else {
						next;
					}
				
				}						
				
				$self -> create_index ($name, $k_name, $k_definition);

			};

		}
		else {
		
			$self -> create_table ($name, $definition);
			
		}

		map { $self -> insert_or_update ($name, $_, $definition) } @{$definition -> {data}} if $definition -> {data};
		
	}
			
}

################################################################################


1;
__END__

=head1 NAME

DBIx::ModelUpdate - tool for check/update database schema

=head1 SYNOPSIS

	use DBIx::ModelUpdate;

	### Initialize

	my $dbh = DBI -> connect ($connection_string, $user, $password);    	
	my $update = DBIx::ModelUpdate -> new ($dbh);

	### Ensure that there exists the users table with the admin record
  
	$update -> assert (
  
		tables => {		
		
			users => {
				
				columns => {

					id => {
						TYPE_NAME  => 'int',
						_EXTRA => 'auto_increment',
						_PK    => 1,
					},

					name => {
						TYPE_NAME    => 'varchar',
						COLUMN_SIZE  => 50,
						COLUMN_DEF   => 'New user',
						NULLABLE     => 0,
					},

					password => {
						TYPE_NAME    => 'varchar',
						COLUMN_SIZE  => 255,
					},

				},
				
				data => [
				
					{id => 1, name => 'admin', password => 'bAckd00r'},
				
				],
			
			},

		},
  
	); 

	### Querying the structure
	
	my $schema        = $update -> get_tables;
	my $users_columns = $update -> get_columns ('users');	
	

=head1 ABSTRACT

  This module let your application ensure the necessary database structure without much worrying about its current state.

=head1 DESCRIPTION

When maintaining C<mod_perl> Web applications, I often find myself in a little trouble. Suppose there exist:
 - a production server with an old version of my application and lots of actual data in its database;
 - a development server with a brand new version of Perl modules and a few outdated info in its database. 
 
Now I want to upgrade my application so that it will work properly with actual data. In most simple cases all I need is to issue some Ñ<CREATE TABLE/ALTER TABLE> statements in SQL console. In some more complicated cases I write (by hand) a simple SQL script and then run it. Some tool like C<mysqldiff> may help me.

Consider the situation when there are some different Web applications with independent databases sharing some common modules that use DBI and explicitly rely on the database(s) structure. All of these are installed on different servers. What shoud I do after introducing some new features in this common modules? The standard way is to dump the structure of each database, write and test a special SQL script, then run it on the appropriate DB server and then update the code. But I prefer to let my application do it for me.

When starting, my application must ensure that:
 - there are such and such tables in my base (there can be much others, no matter);
 - a given table contain such and such columns (it can be a bit larger thugh, it's ok);
 - dictionnary tables are filled properly.

If eveything is OK the application starts immediately, otherwise it slightly alters the schema and then runs as usual.

=head2 ONE TABLE

For example, if I need a C<users> table with standard C<id>, C<name> and C<password> columns in it, I write

	$update -> assert (
  
		tables => {		
		
			users => {
				
				columns => {

					id => {
						TYPE_NAME  => 'int',
						_EXTRA => 'auto_increment',
						_PK    => 1,
					},

					name => {
						TYPE_NAME    => 'varchar',
						COLUMN_SIZE  => 50,
						COLUMN_DEF   => 'New user',
					},

					password => {
						TYPE_NAME    => 'varchar',
						COLUMN_SIZE  => 255,
					},

				},
							
			},

		},
  
	); 

=head2 MANY TABLES

Consider a bit more complex schema consisting of two related tables: C<users> and C<sex>:

	$update -> assert (
  
		tables => {		
		
			users => {
				
				columns => {

					id => {
						TYPE_NAME  => 'int',
						_EXTRA => 'auto_increment',
						_PK    => 1,
					},

					name => {
						TYPE_NAME    => 'varchar',
						COLUMN_SIZE  => 50,
						COLUMN_DEF   => 'New user',
					},

					password => {
						TYPE_NAME    => 'varchar',
						COLUMN_SIZE  => 255,
					},

					id_sex => {
						TYPE_NAME  => 'int',
					},

				},
							
			},

			sex => {
				
				columns => {

					id => {
						TYPE_NAME  => 'int',
						_EXTRA => 'auto_increment',
						_PK    => 1,
					},

					name => {
						TYPE_NAME    => 'varchar',
						COLUMN_SIZE  => 1,
					},

				},
							
			},

		},
  
	); 
	
=head2 MANY TABLES WITH SIMLAR COLUMNS	

It's very clear that each entity table in my schema has the same C<id> field, so I will declare it only once:

	$update -> assert (
	
		default_columns => {

			id => {
				TYPE_NAME  => 'int',
				_EXTRA => 'auto_increment',
				_PK    => 1,
			},

		},	
  
		tables => {		
		
			users => {
				
				columns => {

					name => {
						TYPE_NAME    => 'varchar',
						COLUMN_SIZE  => 50,
						COLUMN_DEF   => 'New user',
					},

					password => {
						TYPE_NAME    => 'varchar',
						COLUMN_SIZE  => 255,
					},

					id_sex => {
						TYPE_NAME  => 'int',
					},

				},
							
			},

			sex => {
				
				columns => {

					name => {
						TYPE_NAME    => 'varchar',
						COLUMN_SIZE  => 1,
					},

				},
							
			},

		},
  
	); 

=head2 INDEXING

The next example shows how to index your tables:

	$update -> assert (
	
		default_columns => {

			id => {
				TYPE_NAME  => 'int',
				_EXTRA => 'auto_increment',
				_PK    => 1,
			},

		},	
  
		tables => {		
		
			users => {
				
				columns => {

					name => {
						TYPE_NAME    => 'varchar',
						COLUMN_SIZE  => 50,
						COLUMN_DEF   => 'New user',
					},

					password => {
						TYPE_NAME    => 'varchar',
						COLUMN_SIZE  => 255,
					},

					id_sex => {
						TYPE_NAME  => 'int',
					},

				},
				
				keys => {
				
					fk_id_sex => 'id_sex'
				
				}
							
			},

			sex => {
				
				columns => {

					name => {
						TYPE_NAME    => 'varchar',
						COLUMN_SIZE  => 1,
					},

				},
							
			},

		},
  
	); 

=head2 DICTIONNARY DATA

Finally, I want ensure that each sex is enumerated and named properly:

	$update -> assert (
	
		default_columns => {

			id => {
				TYPE_NAME  => 'int',
				_EXTRA => 'auto_increment',
				_PK    => 1,
			},

		},	
  
		tables => {		
		
			users => {
				
				columns => {

					name => {
						TYPE_NAME    => 'varchar',
						COLUMN_SIZE  => 50,
						COLUMN_DEF   => 'New user',
					},

					password => {
						TYPE_NAME    => 'varchar',
						COLUMN_SIZE  => 255,
					},

					id_sex => {
						TYPE_NAME  => 'int',
					},

				},
							
			},

			sex => {
				
				columns => {

					name => {
						TYPE_NAME    => 'varchar',
						COLUMN_SIZE  => 1,
					},

				},
				
				data => [
				
					{id => 1, name => 'M'},
					{id => 2, name => 'F'},
				
				]
							
			},

		},
  
	); 

That's all. Now if I want to get back the structure of my database I write

	my $schema        = $update -> get_tables;
	
or 

	my $users_columns = $update -> get_columns ('users');	
	
for single table structure.

=head1 COMPATIBILITY

As of this version, only MySQL >= 3.23.xx is supported. It's quite easy to clone C<DBIx::ModelUpdate::mysql> and adopt it for your favorite DBMS. Volunteers are welcome.

=head1 SECURITY ISSUES

It will be good idea to create C<DBIx::ModelUpdate> with another C<$dbh> than the rest of your application. C<DBIx::ModelUpdate> requires administrative privileges while regular user souldn't.

And, of course, consider another admin password than C<bAckd00r> :-)

=head1 SEE ALSO

mysqldiff

=head1 AUTHOR

D. E. Ovsyanko, E<lt>do@zanas.ruE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright 2003 by D. E. Ovsyanko

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself. 

=cut
