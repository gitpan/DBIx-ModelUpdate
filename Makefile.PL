use ExtUtils::MakeMaker;
WriteMakefile(
    'NAME'		=> 'DBIx::ModelUpdate',
    'VERSION_FROM'	=> 'ModelUpdate.pm',
    'PREREQ_PM'		=> {
	'DBI'			=> '1.21',
    }, 
    ($] >= 5.005 ? 
      (ABSTRACT_FROM => 'ModelUpdate.pm', 
       AUTHOR     => 'D. Ovsyanko <do@zanas.ru>') : ()),
);
