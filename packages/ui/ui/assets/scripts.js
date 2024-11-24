window.dash_clientside = Object.assign({}, window.dash_clientside, {
    ui: {
      mapLabel: (hoverData) => {
        if (hoverData == null) {
            return null;
        }
        // const formatted = `${hoverData.name}: ${hoverData.data_extra}`

        const formatted = [
            {
                namespace: "dash_mantine_components",
                type: "Title",
                props: {
                children: hoverData.name,
                size: "xs",
                },
            },
            {
                namespace: "dash_mantine_components",
                type: "Text",
                props: {
                children: hoverData.data_extra,
                size: "sm",
                },
            },
        ]


        return formatted
        },
    }
})
