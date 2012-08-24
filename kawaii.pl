use strict;
use warnings;
use utf8;
use 5.014000;
use autodie;
use Data::Dumper;
use Text::UnicodeTable::Simple;

binmode STDOUT, ':utf8';

# ref. http://d.hatena.ne.jp/nowokay/20120817

our %TABLES;

package Query {
    use Mouse;
    extends 'Relation';

    sub from {
        my ($class, $table_name) = @_;
        my $table = $TABLES{$table_name} // die "Unknown table: $table_name";
        return Query->new(
            columns => [map { $_->clone } @{$table->columns}],
            taples    => [@{$table->taples}],
        );
    }

    sub select {
        my ($self, @names) = @_;
        my @indexes;
        my @new_columns;
        for my $name (@names) {
            push @new_columns, Column->new(name => $name);
            my $idx = $self->find_column_index($name);
            push @indexes, $idx;
        }

        my @new_taples;
        for my $taple (@{$self->taples}) {
            my @values;
            for my $idx (@indexes) {
                if ($idx < 0+@{$taple}) {
                    push @values, $taple->[$idx];
                } else {
                    push @values, undef;
                }
            }
            push @new_taples, \@values;
        }

        return Query->new(columns => \@new_columns, taples => \@new_taples);
    }

    sub less_than {
        my ($self, $column_name, $value) = @_;
        my $idx = $self->find_column_index($column_name);
        if ($idx >= 0+@{$self->columns}) {
            return Query->new(columns => $self->columns, taples => []);
        }

        my @new_taples;
        for my $taple (@{$self->taples}) {
            if ($taple->[$idx] < $value) {
                push @new_taples, $taple;
            }
        }
        return Query->new(columns => $self->columns, taples => \@new_taples);
    }

    sub left_join {
        my ($self, $table_name, $matching_field) = @_;
        my $table = $TABLES{$table_name} // die "Unknown table: $table_name";

        # make attributes
        my @new_columns = do {
            my @new_columns = @{$self->columns};
            for my $column (@{$table->columns}) {
                push @new_columns, Column->new(parent => $table->name, name => $column->name);
            }
            @new_columns;
        };

        # make taples
        my @new_taples = do {
            my $left_column_idx  = $self->find_column_index($matching_field);
            my $right_column_idx = $table->find_column_index($matching_field);
            if ($left_column_idx >= $self->column_size || $right_column_idx >= $table->column_size) {
                die "matching field not found.";
            }

            # join
            my @new_taples;
            for my $left_taple (@{$self->taples}) {
                my @new_taple = @$left_taple;
                my $left_value = $left_taple->[$left_column_idx];
                if (defined $left_value) {
                    for my $right_taple (@{$table->taples}) {
                        if (0+@$right_taple < $right_column_idx) {
                            next;
                        }
                        if ($left_value eq $right_taple->[$right_column_idx]) {
                            push @new_taple, @{$right_taple};
                            last; # 今回は、タプルの対応は一対一まで
                        }
                    }
                }
                # fill empty columns
                while (@new_taple < @new_columns) {
                    push @new_taple, undef;
                }
                push @new_taples, \@new_taple;
            }
            @new_taples;
        };
        return Query->new(columns => \@new_columns, taples => \@new_taples);
    }
}

package Column {
    use Mouse;
    has name   => (is => 'ro', required => 1);
    has parent => (is => 'ro');
    sub clone {
        my $self = shift;
        return $self->new(%$self);
    }
}

package Relation {
    use Mouse;
    has columns => (
        is => 'ro',
        isa => 'ArrayRef[Column]',
        required => 1,
    );
    has taples => ( is => 'ro', default => sub { +[] } );

    # get a column index from name
    sub find_column_index {
        my ($self, $name) = @_;
        my $column_size = 0+@{$self->{columns}};
        for my $i (0..$column_size-1) {
            if ($self->columns->[$i]->name eq $name) {
                return $i;
            }
        }
        return $column_size;
    }

    sub column_size {
        my $self = shift;
        return 0+@{$self->{columns}};
    }

    sub as_string {
        my $self = shift;
        my $tb = Text::UnicodeTable::Simple->new();
        $tb->set_header(map { $_->name } @{$self->columns});
        for (@{$self->taples}) {
            $tb->add_row( map { defined($_) ? $_ : '*NULL*' } @{$_} );
        }
        return "$tb";
    }
}

package Table {
    use Mouse;
    extends 'Relation';
    has name => (is => 'ro', required => 1);

    sub create {
        my ($class, %args) = @_;
        $args{columns} = [
            map {
                Column->new(name => $_, parent => $args{name})
            }
            @{$args{columns}}
        ];
        my $self = Table->new(%args);
        $TABLES{$self->name} = $self;
        return $self;
    }

    sub insert {
        my ($self, @data) = @_;
        push @{$self->taples}, \@data;
        return $self;
    }
}

my $shohin = Table->create(name => 'shohin', columns => [qw/shohin_id shohin_name kubun_id price/]);
$shohin->insert(1, "りんご",       1, 300)
       ->insert(2, "みかん",       1, 130)
       ->insert(3, "キャベツ",     2, 200)
       ->insert(4, "わかめ",   undef, 250) # 区分がnull
       ->insert(5, "しいたけ",     3, 180); # 該当区分なし

my $kubun = Table->create(name => 'kubun', columns => [qw/kubun_id kubun_name/]);
$kubun->insert(1, 'くだもの')
      ->insert(2, '野菜');

say($shohin->as_string);
say($kubun->as_string);
say(Query->from('shohin')->as_string);
say(Query->from('shohin')->select("shohin_name", "price")->as_string);
say(Query->from("shohin")->less_than("price", 250)->as_string);
say(Query->from('shohin')->left_join('kubun', 'kubun_id')->as_string);
say(Query->from('shohin')->left_join('kubun', 'kubun_id')->less_than('price' => 200)->select('shohin_name', 'kubun_name', 'price')->as_string);

