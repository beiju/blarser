macro_rules! polymorphic_enum {
    ($(#[$meta:meta])* $name:ident: $macro:ident { $($variant:ident($type:path),)* }) => {
        polymorphic_enum! { () $(#[$meta])* $name: $macro { $($variant($type),)* } }
    };
    ($(#[$meta:meta])* pub $name:ident: $macro:ident { $($variant:ident($type:path),)* }) => {
        polymorphic_enum! { (pub) $(#[$meta])* $name: $macro { $($variant($type),)* } }
    };
    ($(#[$meta:meta])* pub(crate) $name:ident: $macro:ident { $($variant:ident($type:path),)* }) => {
        polymorphic_enum! { (pub(crate)) $(#[$meta])* $name: $macro { $($variant($type),)* } }
    };

    (($($vis:tt)*) $(#[$meta:meta])* $name:ident: $macro:ident { $($variant:ident($type:path),)* }) => {
        $(#[$meta])*
        $($vis)* enum $name { $($variant($type)),* }
        macro_rules! $macro {
            ($on:expr, |$with:ident| $body:block) => {
                match $on {
                    $($name::$variant($with) => $body )*
                }
            };
            ($on:expr, |$with:ident: $bound_type:ident| $body:block) => {
                match $on {
                    $($name::$variant($with) => { type $bound_type = $type; $body } )*
                }
            };
            ($on:expr, |_: $bound_type:ident| $body:block) => {
                match $on {
                    $($name::$variant(_) => { type $bound_type = $type; $body } )*
                }
            };
        }
    }
}

pub(crate) use polymorphic_enum;