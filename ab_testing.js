// A/B testing implementation using GrowthBook
(function () {
    // Wait for GrowthBook to be ready
    document.addEventListener('growthbook-ready', function () {
        // Define the experiment
        const experiment = {
            key: 'landing-page-variant',
            variations: ['A', 'B']
        };

        // Run the experiment and get the assigned variant
        const result = window.growthbook.run(experiment);
        const variant = result.value;

        console.log('User assigned to variant:', variant);

        // Load the appropriate variant
        loadVariant(variant);

        // Track experiment exposure
        trackExperiment('landing-page-variant', variant);
    });

    // Load the appropriate variant content
    function loadVariant(variant) {
        const container = document.getElementById('variant-container');

        // Remove loading indicator
        container.innerHTML = '';

        // Create iframe to load variant
        const iframe = document.createElement('iframe');
        iframe.src = `variant_${variant.toLowerCase()}.html`;
        iframe.width = '100%';
        iframe.height = '500px';
        iframe.frameBorder = '0';

        container.appendChild(iframe);

        // Add variant-specific CSS
        const variantCSS = document.createElement('link');
        variantCSS.rel = 'stylesheet';
        variantCSS.href = `css/variant_${variant.toLowerCase()}.css`;
        document.head.appendChild(variantCSS);
    }

    // Track experiment exposure
    function trackExperiment(experimentId, variant) {
        // In a real implementation, this would send data to your analytics system
        // For this demo, we'll just log to console and simulate data collection
        console.log('Tracking experiment exposure:', experimentId, variant);

        // Simulated analytics event
        if (window.tracker) {
            window.tracker.trackEvent('experiment_viewed', {
                experimentId: experimentId,
                variant: variant,
                timestamp: new Date().toISOString()
            });
        }
    }
})();