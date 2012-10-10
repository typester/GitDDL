package GitDDL;
use Any::Moose;

our $VERSION = '0.01';

use Carp;
use DBI;
use File::Spec;
use File::Temp;
use Git::Repository;
use SQL::Translator;
use SQL::Translator::Diff;
use Try::Tiny;

has work_tree => (
    is       => 'ro',
    required => 1,
);

has ddl_file => (
    is       => 'ro',
    required => 1,
);

has dsn => (
    is       => 'ro',
    required => 1,
);

has version_table => (
    is      => 'rw',
    default => 'git_ddl_version',
);

has _dbh => (
    is      => 'rw',
    lazy    => 1,
    builder => '_build_dbh',
);

has _git => (
    is      => 'rw',,
    lazy    => 1,
    builder => '_build_git',
);

no Any::Moose;

sub check_version {
    my ($self) = @_;
    $self->database_version eq $self->ddl_version;
}

sub database_version {
    my ($self) = @_;

    croak sprintf 'invalid version_table: %s', $self->version_table
        unless $self->version_table =~ /^[a-zA-Z_]+$/;

    my ($version) =
        $self->_dbh->selectrow_array('SELECT version FROM ' . $self->version_table);

    if (defined $version) {
        return $version;
    }
    else {
        croak "Failed to get database version, please deploy first";
    }
}

sub ddl_version {
    my ($self) = @_;
    $self->_git->run('log', '-n', '1', '--pretty=format:%H', '--', $self->ddl_file);
}

sub deploy {
    my ($self) = @_;

    my $version = try {
        open my $fh, '>', \my $stderr;
        local *STDERR = $fh;
        $self->database_version;
        close $fh;
    };

    if ($version) {
        croak "database already deployed, use upgrade_database instead";
    }

    croak sprintf 'invalid version_table: %s', $self->version_table
        unless $self->version_table =~ /^[a-zA-Z_]+$/;

    $self->_do_sql($self->_slurp(File::Spec->catfile($self->work_tree, $self->ddl_file)));

    $self->_do_sql(<<"__SQL__");
CREATE TABLE @{[ $self->version_table ]} (
    version VARCHAR(40) NOT NULL
);
__SQL__

    $self->_dbh->do(
        "INSERT INTO @{[ $self->version_table ]} (version) VALUES (?)", {}, $self->ddl_version
    ) or croak $self->_dbh->errstr;
}

sub diff {
    my ($self) = @_;

    if ($self->check_version) {
        croak 'ddl_version == database_version, should no differences';
    }

    my $dsn0 = $self->dsn->[0];
    my $db
        = $dsn0 =~ /:mysql:/ ? 'MySQL'
        : $dsn0 =~ /:Pg:/    ? 'PostgreSQL'
        :                      do { my ($d) = $dsn0 =~ /dbi:(.*?):/; $d };

    my $tmp_fh = File::Temp->new;
    $self->_dump_sql_for_specified_coomit($self->database_version, $tmp_fh->filename);

    my $source = SQL::Translator->new;
    $source->parser($db) or croak $source->error;
    $source->translate($tmp_fh->filename) or croak $source->error;

    my $target = SQL::Translator->new;
    $target->parser($db) or croak $target->error;
    $target->translate(File::Spec->catfile($self->work_tree, $self->ddl_file))
        or croak $target->error;

    my $diff = SQL::Translator::Diff->new({
        output_db     => $db,
        source_schema => $source->schema,
        target_schema => $target->schema,
    })->compute_differences->produce_diff_sql;

    # ignore first line
    $diff =~ s/.*?\n//;

    $diff
}

sub upgrade_database {
    my ($self) = @_;

    $self->_do_sql($self->diff);

    $self->_dbh->do(
        "UPDATE @{[ $self->version_table ]} SET version = ?", {}, $self->ddl_version
    ) or croak $self->_dbh->errstr;
}

sub _build_dbh {
    my ($self) = @_;

    # support on_connect_do
    my $on_connect_do;
    if (ref $self->dsn->[-1] eq 'HASH') {
        $on_connect_do = delete $self->dsn->[-1]{on_connect_do};
    }

    my $dbh = DBI->connect(@{ $self->dsn })
        or croak $DBI::errstr;

    if ($on_connect_do) {
        if (ref $on_connect_do eq 'ARRAY') {
            $dbh->do($_) || croak $dbh->errstr
                for @$on_connect_do;
        }
        else {
            $dbh->do($on_connect_do) or croak $dbh->errstr;
        }
    }

    $dbh;
}

sub _build_git {
    my ($self) = @_;
    Git::Repository->new( work_tree => $self->work_tree );
}

sub _do_sql {
    my ($self, $sql) = @_;

    my @statements = map { "$_;" } grep { /\S+/ } split ';', $sql;
    for my $statement (@statements) {
        $self->_dbh->do($statement)
            or croak $self->_dbh->errstr;
    }
}

sub _slurp {
    my ($self, $file) = @_;

    open my $fh, '<', $file or croak sprintf 'Cannot open file: %s, %s', $file, $!;
    my $data = do { local $/; <$fh> };
    close $fh;

    $data;
}

sub _dump_sql_for_specified_coomit {
    my ($self, $commit_hash, $outfile) = @_;

    my ($mode, $type, $blob_hash) = split /\s+/, scalar $self->_git->run(
        'ls-tree', $commit_hash, '--', $self->ddl_file,
    );

    my $sql = $self->_git->run('cat-file', 'blob', $blob_hash);

    open my $fh, '>', $outfile or croak $!;
    print $fh $sql;
    close $fh;
}

__PACKAGE__->meta->make_immutable;

__END__

=head1 NAME

GitDDL - 

=head1 SYNOPSIS

my $gd = GitDDL->new(
    work_dir => '/path/to/project', # git working directory
    ddl_file => 'sql/schema.ddl',
    dsn      => ['dbi:mysql:my_project', 'root', ''],
);

# checking whether the database version matchs ddl_file version or not.
$gd->check_version;

# getting database version
my $db_version = $gd->database_version;

# getting ddl version
my $ddl_version = $gd->ddl_version;

# upgrade database
$gd->upgrade_database;

# deploy ddl
$gd->deploy;

=head1 DESCRIPTION

=head1 AUTHOR

Daisuke Murase <typester@cpan.org>

=head1 COPYRIGHT AND LICENSE

Copyright (c) 2012 Daisuke Murase. All rights reserved.

This program is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

The full text of the license can be found in the LICENSE file included with this module.

=cut
