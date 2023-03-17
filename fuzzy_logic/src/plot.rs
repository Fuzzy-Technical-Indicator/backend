use std::error::Error;

use plotters::{
    prelude::{
        ChartBuilder, IntoDrawingArea, LabelAreaPosition,
        PathElement, SVGBackend,
    },
    series::LineSeries,
    style::{Color, FontStyle, IntoFont, Palette, Palette99, BLACK, WHITE},
};

use crate::{linguistic::LinguisticVar, set::FuzzySet};

const FONT: &str = "Arial";

pub fn plot_linguistic(var: &LinguisticVar, name: &str, path: &str) -> Result<(), Box<dyn Error>> {
    let filepath = format!("{path}/{name}.svg");
    let root = SVGBackend::new(&filepath, (1024, 768)).into_drawing_area();
    root.fill(&WHITE)?;

    let mut chart = ChartBuilder::on(&root)
        .caption(name, (FONT, 44, FontStyle::Bold).into_font())
        .set_label_area_size(LabelAreaPosition::Left, 60)
        .set_label_area_size(LabelAreaPosition::Bottom, 60)
        .margin(60)
        .build_cartesian_2d(
            *var.universe.first().unwrap()..*var.universe.last().unwrap(),
            0f64..1f64,
        )?;

    chart
        .configure_mesh()
        .disable_x_mesh()
        .y_max_light_lines(0)
        .x_labels(5)
        .y_labels(5)
        .x_label_style((FONT, 30).into_font())
        .y_label_style((FONT, 30).into_font())
        .draw()?;

    for (i, (k, v)) in var.sets.iter().enumerate() {
        let color = Palette99::pick(i);
        chart
            .draw_series(LineSeries::new(
                var.universe.iter().map(|x| (*x, v.degree_of(*x))),
                color.mix(0.5).stroke_width(4),
            ))?
            .label(k)
            .legend(move |(x, y)| {
                PathElement::new([(x, y), (x + 20, y)], color.filled().stroke_width(4))
            });
    }
    chart
        .configure_series_labels()
        .label_font((FONT, 30).into_font())
        .background_style(&WHITE)
        .border_style(&BLACK)
        .draw()?;

    root.present()?;
    Ok(())
}

pub fn plot_set(set: &FuzzySet, name: &str, path: &str) -> Result<(), Box<dyn Error>> {
    let filepath = format!("{path}/{name}.svg");
    let root = SVGBackend::new(&filepath, (1024, 768)).into_drawing_area();
    root.fill(&WHITE)?;

    let mut chart = ChartBuilder::on(&root)
        .caption(name, (FONT, 44, FontStyle::Bold).into_font())
        .set_label_area_size(LabelAreaPosition::Left, 60)
        .set_label_area_size(LabelAreaPosition::Bottom, 60)
        .margin(60)
        .build_cartesian_2d(
            *set.universe.first().unwrap()..*set.universe.last().unwrap(),
            0f64..1f64,
        )?;

    chart
        .configure_mesh()
        .disable_x_mesh()
        .y_max_light_lines(0)
        .x_labels(5)
        .y_labels(5)
        .x_label_style((FONT, 30).into_font())
        .y_label_style((FONT, 30).into_font())
        .draw()?;

    let color = Palette99::pick(0);
    chart
        .draw_series(LineSeries::new(
            set.universe.iter().map(|x| (*x, set.degree_of(*x))),
            color.stroke_width(2),
        ))?
        .label(name)
        .legend(move |(x, y)| {
            PathElement::new([(x, y), (x + 20, y)], color.filled().stroke_width(4))
        });

    chart
        .configure_series_labels()
        .label_font((FONT, 30).into_font())
        .background_style(&WHITE)
        .border_style(&BLACK)
        .draw()?;

    root.present()?;
    Ok(())
}
