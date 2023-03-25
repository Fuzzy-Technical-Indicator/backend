use std::error::Error;

use plotters::{
    coord::types::RangedCoordf64,
    prelude::{
        Cartesian2d, ChartBuilder, ChartContext, CoordTranslate, DrawingBackend, IntoDrawingArea,
        LabelAreaPosition, PathElement, SVGBackend,
    },
    series::LineSeries,
    style::{Color, FontStyle, IntoFont, Palette, Palette99, BLACK, WHITE},
};

use crate::{linguistic::LinguisticVar, set::FuzzySet};

const FONT: &str = "Arial";

fn config_chart<'a, DB: DrawingBackend>(
    chart: &'a mut ChartBuilder<'a, '_, DB>,
    title: &str,
    universe: (f64, f64),
) -> ChartContext<'a, DB, Cartesian2d<RangedCoordf64, RangedCoordf64>> {
    chart
        .caption(title, (FONT, 44, FontStyle::Bold).into_font())
        .set_label_area_size(LabelAreaPosition::Left, 60)
        .set_label_area_size(LabelAreaPosition::Bottom, 60)
        .margin(60);

    let mut c = chart
        .build_cartesian_2d(universe.0..universe.1, 0f64..1f64)
        .unwrap();

    c.configure_mesh()
        .disable_x_mesh()
        .y_max_light_lines(0)
        .x_labels(5)
        .y_labels(5)
        .x_label_style((FONT, 30).into_font())
        .y_label_style((FONT, 30).into_font())
        .draw()
        .unwrap();
    c
}

fn config_series_label<'a, DB: DrawingBackend + 'a, CT: CoordTranslate>(
    chart: &mut ChartContext<'a, DB, CT>,
) {
    chart
        .configure_series_labels()
        .label_font((FONT, 30).into_font())
        .background_style(&WHITE)
        .border_style(&BLACK)
        .draw()
        .unwrap();
}

pub fn plot_linguistic(var: &LinguisticVar, title: &str, path: &str) -> Result<(), Box<dyn Error>> {
    let root = SVGBackend::new(path, (1024, 768)).into_drawing_area();
    root.fill(&WHITE)?;

    let mut chart_b = ChartBuilder::on(&root);
    let mut chart = config_chart(&mut chart_b, title, var.universe);

    for (i, (k, v)) in var.sets.iter().enumerate() {
        let color = Palette99::pick(i);
        chart
            .draw_series(LineSeries::new(
                var.get_finite_universe(0.01)
                    .iter()
                    .map(|x| (*x, v.degree_of(*x))),
                color.mix(0.5).stroke_width(4),
            ))?
            .label(k)
            .legend(move |(x, y)| {
                PathElement::new([(x, y), (x + 20, y)], color.filled().stroke_width(4))
            });
    }

    config_series_label(&mut chart);
    root.present()?;
    Ok(())
}

pub fn plot_set(set: &FuzzySet, title: &str, path: &str) -> Result<(), Box<dyn Error>> {
    let root = SVGBackend::new(path, (1024, 768)).into_drawing_area();
    root.fill(&WHITE)?;

    let mut chart_b = ChartBuilder::on(&root);
    let mut chart = config_chart(&mut chart_b, title, set.universe);

    let color = Palette99::pick(0);
    chart
        .draw_series(LineSeries::new(
            set.get_finite_universe(0.01)
                .iter()
                .map(|x| (*x, set.degree_of(*x))),
            color.stroke_width(4),
        ))?
        .label(title)
        .legend(move |(x, y)| {
            PathElement::new([(x, y), (x + 20, y)], color.filled().stroke_width(4))
        });

    config_series_label(&mut chart);
    root.present()?;
    Ok(())
}
